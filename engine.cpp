#include "engine.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <ctime>
#include <iomanip>
#include <regex>
#include <sstream>
#include <stdexcept>

namespace flexql {

namespace {

std::string qualify_name(const std::string &table, const std::string &column) {
    return table + "." + column;
}

std::vector<std::string> tokenize_projection(const std::vector<std::string> &columns) {
    auto trim_copy = [](const std::string &input) {
        std::size_t start = 0;
        while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
            ++start;
        }
        std::size_t end = input.size();
        while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
            --end;
        }
        return input.substr(start, end - start);
    };
    std::vector<std::string> trimmed;
    trimmed.reserve(columns.size());
    for (const std::string &column : columns) {
        trimmed.push_back(trim_copy(column));
    }
    return trimmed;
}

std::tm tm_from_parts(int year, int month, int day, int hour, int minute, int second) {
    std::tm tm_value{};
    tm_value.tm_year = year - 1900;
    tm_value.tm_mon = month - 1;
    tm_value.tm_mday = day;
    tm_value.tm_hour = hour;
    tm_value.tm_min = minute;
    tm_value.tm_sec = second;
    tm_value.tm_isdst = -1;
    return tm_value;
}

}  // namespace

Engine::QueryCache::QueryCache(std::size_t capacity)
    : capacity_(capacity) {}

std::optional<QueryResult> Engine::QueryCache::get(const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = lookup_.find(key);
    if (it == lookup_.end()) {
        return std::nullopt;
    }
    order_.splice(order_.begin(), order_, it->second);
    return it->second->second;
}

void Engine::QueryCache::put(const std::string &key, const QueryResult &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = lookup_.find(key);
    if (it != lookup_.end()) {
        it->second->second = value;
        order_.splice(order_.begin(), order_, it->second);
        return;
    }
    order_.push_front({key, value});
    lookup_[key] = order_.begin();
    if (order_.size() > capacity_) {
        auto last = std::prev(order_.end());
        lookup_.erase(last->first);
        order_.pop_back();
    }
}

void Engine::QueryCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    order_.clear();
    lookup_.clear();
}

Engine::Engine()
    : cache_(128) {}

QueryResult Engine::execute(const std::string &sql) {
    const std::string cleaned = trim(sql);
    if (cleaned.empty()) {
        throw SqlError("empty SQL statement");
    }

    const std::string upper = to_upper(cleaned);
    if (upper.rfind("CREATE TABLE", 0) == 0) {
        auto stmt = parse_create(cleaned);
        return execute_create(stmt);
    }
    if (upper.rfind("INSERT INTO", 0) == 0) {
        auto stmt = parse_insert(cleaned);
        return execute_insert(stmt);
    }
    if (upper.rfind("SELECT", 0) == 0) {
        const std::string key = canonicalize_sql(cleaned);
        if (auto cached = cache_.get(key)) {
            return *cached;
        }
        auto stmt = parse_select(cleaned);
        QueryResult result = execute_select(stmt, key);
        cache_.put(key, result);
        return result;
    }

    throw SqlError("unsupported SQL statement");
}

QueryResult Engine::execute_create(const CreateTableStatement &stmt) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    const std::string table_name = to_upper(stmt.table_name);
    if (tables_.count(table_name) != 0U) {
        throw SqlError("table already exists: " + stmt.table_name);
    }
    if (stmt.columns.empty()) {
        throw SqlError("CREATE TABLE requires at least one column");
    }

    Table table;
    int primary_count = 0;
    for (std::size_t i = 0; i < stmt.columns.size(); ++i) {
        Column column = stmt.columns[i];
        column.name = to_upper(column.name);
        if (table.column_index.count(column.name) != 0U) {
            throw SqlError("duplicate column name: " + column.name);
        }
        table.column_index[column.name] = i;
        if (column.primary_key) {
            table.primary_key_index = static_cast<int>(i);
            ++primary_count;
        }
        table.columns.push_back(column);
    }
    if (primary_count > 1) {
        throw SqlError("only one PRIMARY KEY column is supported");
    }
    tables_[table_name] = std::move(table);
    invalidate_cache();
    return QueryResult{{}, {}, "table created"};
}

QueryResult Engine::execute_insert(const InsertStatement &stmt) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    Table &table = require_table(stmt.table_name);
    purge_expired_rows(table);

    std::vector<std::string> values = stmt.values;
    if (values.size() != table.columns.size()) {
        throw SqlError("value count does not match table schema");
    }
    validate_and_normalize_row(table, values);

    if (table.primary_key_index >= 0) {
        const std::string &key = values[static_cast<std::size_t>(table.primary_key_index)];
        if (table.primary_index.count(key) != 0U) {
            throw SqlError("duplicate primary key value: " + key);
        }
    }

    table.rows.push_back(Row{values, stmt.expires_at_epoch_seconds, false});
    if (table.primary_key_index >= 0) {
        const std::string &key = values[static_cast<std::size_t>(table.primary_key_index)];
        table.primary_index[key] = table.rows.size() - 1;
    }
    invalidate_cache();
    return QueryResult{{}, {}, "1 row inserted"};
}

QueryResult Engine::execute_select(const SelectStatement &stmt, const std::string &cache_key) {
    (void) cache_key;
    std::unique_lock<std::shared_mutex> lock(mutex_);

    Table &left_table = require_table(stmt.left_table);
    purge_expired_rows(left_table);

    Table *right_table = nullptr;
    if (stmt.right_table.has_value()) {
        right_table = &require_table(*stmt.right_table);
        purge_expired_rows(*right_table);
    }

    QueryResult result;
    std::vector<std::string> projection = tokenize_projection(stmt.selected_columns);

    auto resolve_projection = [&](const std::unordered_map<std::string, std::string> &field_map) {
        std::vector<std::string> row;
        if (projection.size() == 1 && projection[0] == "*") {
            row.reserve(field_map.size());
            if (right_table == nullptr) {
                if (result.column_names.empty()) {
                    for (const Column &column : left_table.columns) {
                        result.column_names.push_back(column.name);
                    }
                }
                for (const Column &column : left_table.columns) {
                    row.push_back(field_map.at(column.name));
                }
            } else {
                if (result.column_names.empty()) {
                    for (const Column &column : left_table.columns) {
                        result.column_names.push_back(qualify_name(to_upper(stmt.left_table), column.name));
                    }
                    for (const Column &column : right_table->columns) {
                        result.column_names.push_back(qualify_name(to_upper(*stmt.right_table), column.name));
                    }
                }
                for (const Column &column : left_table.columns) {
                    row.push_back(field_map.at(qualify_name(to_upper(stmt.left_table), column.name)));
                }
                for (const Column &column : right_table->columns) {
                    row.push_back(field_map.at(qualify_name(to_upper(*stmt.right_table), column.name)));
                }
            }
            return row;
        }

        if (result.column_names.empty()) {
            result.column_names = projection;
        }
        row.reserve(projection.size());
        for (const std::string &name : projection) {
            auto found = field_map.find(to_upper(name));
            if (found == field_map.end()) {
                throw SqlError("unknown column in SELECT: " + name);
            }
            row.push_back(found->second);
        }
        return row;
    };

    const std::string left_name = to_upper(stmt.left_table);
    const std::string right_name = stmt.right_table ? to_upper(*stmt.right_table) : "";

    for (const Row &left_row : left_table.rows) {
        if (left_row.deleted) {
            continue;
        }

        auto build_left_map = [&]() {
            std::unordered_map<std::string, std::string> field_map;
            for (std::size_t i = 0; i < left_table.columns.size(); ++i) {
                const std::string &column_name = left_table.columns[i].name;
                field_map[column_name] = left_row.values[i];
                field_map[qualify_name(left_name, column_name)] = left_row.values[i];
            }
            return field_map;
        };

        if (right_table == nullptr) {
            auto field_map = build_left_map();
            if (stmt.where && !evaluate_condition(*stmt.where, field_map)) {
                continue;
            }
            result.rows.push_back(resolve_projection(field_map));
            continue;
        }

        for (const Row &right_row : right_table->rows) {
            if (right_row.deleted) {
                continue;
            }
            auto field_map = build_left_map();
            for (std::size_t i = 0; i < right_table->columns.size(); ++i) {
                const std::string &column_name = right_table->columns[i].name;
                field_map[qualify_name(right_name, column_name)] = right_row.values[i];
            }

            bool join_matches = true;
            if (stmt.join_condition) {
                join_matches = evaluate_condition(*stmt.join_condition, field_map);
            }
            if (!join_matches) {
                continue;
            }
            if (stmt.where && !evaluate_condition(*stmt.where, field_map)) {
                continue;
            }
            result.rows.push_back(resolve_projection(field_map));
        }
    }

    if (result.message.empty()) {
        result.message = std::to_string(result.rows.size()) + " row(s)";
    }
    return result;
}

CreateTableStatement Engine::parse_create(const std::string &sql) const {
    static const std::regex pattern(
        R"(^\s*CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*;?\s*$)",
        std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, pattern)) {
        throw SqlError("invalid CREATE TABLE syntax");
    }

    CreateTableStatement stmt;
    stmt.table_name = match[1].str();
    for (const std::string &entry : split_csv(match[2].str())) {
        std::istringstream stream(entry);
        Column column;
        std::string type_token;
        std::string pk_token1;
        std::string pk_token2;
        if (!(stream >> column.name >> type_token)) {
            throw SqlError("invalid column definition: " + entry);
        }
        std::string type_upper = to_upper(type_token);
        std::size_t paren_pos = type_upper.find('(');
        if (paren_pos != std::string::npos) {
            type_upper = type_upper.substr(0, paren_pos);
        }
        if (type_upper == "INT") {
            column.type = ColumnType::Int;
        } else if (type_upper == "DECIMAL") {
            column.type = ColumnType::Decimal;
        } else if (type_upper == "VARCHAR") {
            column.type = ColumnType::Varchar;
        } else if (type_upper == "DATETIME") {
            column.type = ColumnType::Datetime;
        } else {
            throw SqlError("unsupported column type: " + type_token);
        }
        if (stream >> pk_token1) {
            if (!(stream >> pk_token2) || to_upper(pk_token1) != "PRIMARY" || to_upper(pk_token2) != "KEY") {
                throw SqlError("expected PRIMARY KEY after column type");
            }
            column.primary_key = true;
        }
        stmt.columns.push_back(column);
    }
    return stmt;
}

InsertStatement Engine::parse_insert(const std::string &sql) const {
    static const std::regex pattern(
        R"(^\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)\s+VALUES\s*\((.*?)\)\s*(.*?)\s*;?\s*$)",
        std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, pattern)) {
        throw SqlError("invalid INSERT syntax");
    }

    InsertStatement stmt;
    stmt.table_name = match[1].str();
    stmt.values = split_csv(match[2].str());
    for (std::string &value : stmt.values) {
        value = unquote(trim(value));
    }

    std::string tail = trim(match[3].str());
    if (!tail.empty() && tail.back() == ';') {
        tail.pop_back();
        tail = trim(tail);
    }
    if (!tail.empty()) {
        static const std::regex expires_at_pattern(
            R"(^EXPIRES\s+AT\s+(.+)$)",
            std::regex::icase);
        static const std::regex ttl_pattern(
            R"(^TTL\s+([0-9]+)$)",
            std::regex::icase);
        std::smatch tail_match;
        if (std::regex_match(tail, tail_match, expires_at_pattern)) {
            auto parsed = parse_timestamp_literal(unquote(trim(tail_match[1].str())));
            if (!parsed.has_value()) {
                throw SqlError("invalid expiration timestamp");
            }
            stmt.expires_at_epoch_seconds = parsed;
        } else if (std::regex_match(tail, tail_match, ttl_pattern)) {
            long long ttl_seconds = std::stoll(tail_match[1].str());
            const auto now = std::chrono::system_clock::now();
            const auto epoch = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
            stmt.expires_at_epoch_seconds = epoch + ttl_seconds;
        } else {
            throw SqlError("unsupported INSERT expiration clause");
        }
    }

    return stmt;
}

SelectStatement Engine::parse_select(const std::string &sql) const {
    static const std::regex pattern(
        R"(^\s*SELECT\s+(.*?)\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+INNER\s+JOIN\s+([A-Za-z_][A-Za-z0-9_]*)\s+ON\s+(.*?))?(?:\s+WHERE\s+(.*?))?\s*;?\s*$)",
        std::regex::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, pattern)) {
        throw SqlError("invalid SELECT syntax");
    }

    auto parse_condition = [&](const std::string &expr, bool allow_column_rhs) -> Condition {
        static const std::regex cond_pattern(
            R"(^\s*([A-Za-z_][A-Za-z0-9_\.]*)\s*(=|!=|<=|>=|<|>)\s*(.*?)\s*$)",
            std::regex::icase);
        std::smatch cond_match;
        if (!std::regex_match(expr, cond_match, cond_pattern)) {
            throw SqlError("invalid condition: " + expr);
        }
        Condition condition;
        condition.left = to_upper(trim(cond_match[1].str()));
        auto op = parse_compare_op(cond_match[2].str());
        if (!op.has_value()) {
            throw SqlError("unsupported comparison operator");
        }
        condition.op = *op;
        std::string right = trim(cond_match[3].str());
        if (allow_column_rhs && right.find('\'') == std::string::npos && right.find('"') == std::string::npos &&
            right.find('.') != std::string::npos && !is_number_literal(right)) {
            condition.right = to_upper(right);
            condition.right_is_column = true;
        } else if (allow_column_rhs && std::regex_match(right, std::regex(R"([A-Za-z_][A-Za-z0-9_\.]*)"))) {
            condition.right = to_upper(right);
            condition.right_is_column = true;
        } else {
            condition.right = unquote(right);
        }
        return condition;
    };

    SelectStatement stmt;
    stmt.selected_columns = split_csv(match[1].str());
    stmt.left_table = match[2].str();
    if (match[3].matched) {
        stmt.right_table = match[3].str();
        stmt.join_condition = parse_condition(match[4].str(), true);
    }
    if (match[5].matched) {
        stmt.where = parse_condition(match[5].str(), false);
    }
    return stmt;
}

std::string Engine::trim(const std::string &input) {
    std::size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        ++start;
    }
    std::size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return input.substr(start, end - start);
}

std::string Engine::to_upper(std::string input) {
    std::transform(input.begin(), input.end(), input.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return input;
}

std::vector<std::string> Engine::split_csv(const std::string &text) {
    std::vector<std::string> parts;
    std::string current;
    bool in_single_quote = false;
    bool in_double_quote = false;

    for (char ch : text) {
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
            current.push_back(ch);
            continue;
        }
        if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            current.push_back(ch);
            continue;
        }
        if (ch == ',' && !in_single_quote && !in_double_quote) {
            parts.push_back(trim(current));
            current.clear();
            continue;
        }
        current.push_back(ch);
    }

    if (!current.empty()) {
        parts.push_back(trim(current));
    }
    return parts;
}

std::optional<CompareOp> Engine::parse_compare_op(const std::string &token) {
    if (token == "=") {
        return CompareOp::Eq;
    }
    if (token == "!=") {
        return CompareOp::Ne;
    }
    if (token == "<") {
        return CompareOp::Lt;
    }
    if (token == "<=") {
        return CompareOp::Le;
    }
    if (token == ">") {
        return CompareOp::Gt;
    }
    if (token == ">=") {
        return CompareOp::Ge;
    }
    return std::nullopt;
}

std::optional<std::int64_t> Engine::parse_timestamp_literal(const std::string &text) {
    std::tm tm_value{};
    std::istringstream stream(text);
    stream >> std::get_time(&tm_value, "%Y-%m-%d %H:%M:%S");
    if (!stream.fail()) {
        tm_value.tm_isdst = -1;
        return static_cast<std::int64_t>(std::mktime(&tm_value));
    }

    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (std::sscanf(text.c_str(), "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second) == 6) {
        std::tm parsed = tm_from_parts(year, month, day, hour, minute, second);
        return static_cast<std::int64_t>(std::mktime(&parsed));
    }

    std::int64_t numeric = 0;
    auto begin = text.data();
    auto end = text.data() + text.size();
    if (std::from_chars(begin, end, numeric).ec == std::errc()) {
        return numeric;
    }
    return std::nullopt;
}

std::string Engine::canonicalize_sql(const std::string &sql) {
    std::string canonical;
    canonical.reserve(sql.size());
    bool previous_space = false;
    for (char ch : sql) {
        if (std::isspace(static_cast<unsigned char>(ch))) {
            if (!previous_space) {
                canonical.push_back(' ');
            }
            previous_space = true;
            continue;
        }
        canonical.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
        previous_space = false;
    }
    return trim(canonical);
}

bool Engine::is_identifier_char(char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

bool Engine::is_number_literal(const std::string &value) {
    if (value.empty()) {
        return false;
    }
    bool dot_seen = false;
    std::size_t start = (value[0] == '-') ? 1U : 0U;
    if (start == value.size()) {
        return false;
    }
    for (std::size_t i = start; i < value.size(); ++i) {
        if (value[i] == '.') {
            if (dot_seen) {
                return false;
            }
            dot_seen = true;
            continue;
        }
        if (!std::isdigit(static_cast<unsigned char>(value[i]))) {
            return false;
        }
    }
    return true;
}

std::string Engine::unquote(const std::string &value) {
    std::string trimmed = trim(value);
    if (trimmed.size() >= 2U) {
        if ((trimmed.front() == '\'' && trimmed.back() == '\'') ||
            (trimmed.front() == '"' && trimmed.back() == '"')) {
            return trimmed.substr(1, trimmed.size() - 2U);
        }
    }
    return trimmed;
}

const Engine::Table &Engine::require_table(const std::string &table_name) const {
    const std::string normalized = to_upper(table_name);
    auto it = tables_.find(normalized);
    if (it == tables_.end()) {
        throw SqlError("unknown table: " + table_name);
    }
    return it->second;
}

Engine::Table &Engine::require_table(const std::string &table_name) {
    const std::string normalized = to_upper(table_name);
    auto it = tables_.find(normalized);
    if (it == tables_.end()) {
        throw SqlError("unknown table: " + table_name);
    }
    return it->second;
}

void Engine::validate_and_normalize_row(const Table &table, std::vector<std::string> &values) const {
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        const Column &column = table.columns[i];
        std::string &value = values[i];
        if (column.type == ColumnType::Int) {
            if (!is_number_literal(value) || value.find('.') != std::string::npos) {
                throw SqlError("expected INT for column " + column.name);
            }
        } else if (column.type == ColumnType::Decimal) {
            if (!is_number_literal(value)) {
                throw SqlError("expected DECIMAL for column " + column.name);
            }
        } else if (column.type == ColumnType::Datetime) {
            auto parsed = parse_timestamp_literal(value);
            if (!parsed.has_value()) {
                throw SqlError("expected DATETIME for column " + column.name);
            }
            value = std::to_string(*parsed);
        }
    }
}

void Engine::purge_expired_rows(Table &table) {
    const auto now = std::chrono::system_clock::now();
    const auto epoch_now = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    bool removed_any = false;
    for (std::size_t i = 0; i < table.rows.size(); ++i) {
        Row &row = table.rows[i];
        if (row.deleted || !row.expires_at_epoch_seconds.has_value()) {
            continue;
        }
        if (*row.expires_at_epoch_seconds <= epoch_now) {
            row.deleted = true;
            if (table.primary_key_index >= 0) {
                table.primary_index.erase(row.values[static_cast<std::size_t>(table.primary_key_index)]);
            }
            removed_any = true;
        }
    }
    if (removed_any) {
        invalidate_cache();
    }
}

void Engine::invalidate_cache() {
    cache_.clear();
}

bool Engine::evaluate_condition(
    const Condition &condition,
    const std::unordered_map<std::string, std::string> &field_map) const {
    auto left_it = field_map.find(to_upper(condition.left));
    if (left_it == field_map.end()) {
        throw SqlError("unknown column in condition: " + condition.left);
    }
    const std::string left = left_it->second;

    std::string right;
    if (condition.right_is_column) {
        auto right_it = field_map.find(to_upper(condition.right));
        if (right_it == field_map.end()) {
            throw SqlError("unknown column in condition: " + condition.right);
        }
        right = right_it->second;
    } else {
        right = condition.right;
    }

    auto compare_numeric = [&](double lhs, double rhs) {
        switch (condition.op) {
            case CompareOp::Eq: return lhs == rhs;
            case CompareOp::Ne: return lhs != rhs;
            case CompareOp::Lt: return lhs < rhs;
            case CompareOp::Le: return lhs <= rhs;
            case CompareOp::Gt: return lhs > rhs;
            case CompareOp::Ge: return lhs >= rhs;
        }
        return false;
    };

    if (is_number_literal(left) && is_number_literal(right)) {
        return compare_numeric(std::stod(left), std::stod(right));
    }

    switch (condition.op) {
        case CompareOp::Eq: return left == right;
        case CompareOp::Ne: return left != right;
        case CompareOp::Lt: return left < right;
        case CompareOp::Le: return left <= right;
        case CompareOp::Gt: return left > right;
        case CompareOp::Ge: return left >= right;
    }
    return false;
}

}  // namespace flexql
