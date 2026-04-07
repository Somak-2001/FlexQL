#include "engine.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>
#include <stdexcept>

namespace flexql {

namespace {

constexpr std::size_t kCheckpointRowThreshold = 20000000;

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

std::string trim_copy(const std::string &input) {
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

bool write_all_fd(int fd, const std::string &content) {
    const char *cursor = content.data();
    std::size_t remaining = content.size();
    while (remaining > 0) {
        ssize_t written = ::write(fd, cursor, remaining);
        if (written < 0) {
            return false;
        }
        cursor += written;
        remaining -= static_cast<std::size_t>(written);
    }
    return true;
}

void sync_parent_directory(const std::filesystem::path &path) {
    const std::filesystem::path parent = path.parent_path().empty()
        ? std::filesystem::current_path()
        : path.parent_path();
    int dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) {
        return;
    }
    ::fsync(dir_fd);
    ::close(dir_fd);
}

void write_file_atomic(const std::filesystem::path &path, const std::string &content) {
    const std::filesystem::path tmp_path = path.string() + ".tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw SqlError("failed to open persistence file: " + tmp_path.string());
    }
    if (!write_all_fd(fd, content) || ::fsync(fd) != 0) {
        ::close(fd);
        throw SqlError("failed to write persistence file: " + tmp_path.string());
    }
    ::close(fd);
    std::filesystem::rename(tmp_path, path);
    sync_parent_directory(path);
}

std::string sql_quote(const std::string &value) {
    return "'" + value + "'";
}

std::string format_sql_value(const flexql::Column &column, const std::string &value) {
    if (column.type == flexql::ColumnType::Varchar) {
        return sql_quote(value);
    }
    return value;
}

std::vector<std::vector<std::string>> parse_insert_rows(const std::string &values_text) {
    std::vector<std::vector<std::string>> rows;
    std::size_t pos = 0;

    auto skip_space = [&](void) {
        while (pos < values_text.size() && std::isspace(static_cast<unsigned char>(values_text[pos]))) {
            ++pos;
        }
    };

    skip_space();
    while (pos < values_text.size()) {
        if (values_text[pos] != '(') {
            throw flexql::SqlError("invalid INSERT syntax");
        }
        ++pos;

        std::vector<std::string> row;
        while (pos < values_text.size()) {
            skip_space();
            if (pos >= values_text.size()) {
                throw flexql::SqlError("invalid INSERT syntax");
            }

            std::string value;
            if (values_text[pos] == '\'' || values_text[pos] == '"') {
                const char quote = values_text[pos++];
                std::size_t start = pos;
                while (pos < values_text.size() && values_text[pos] != quote) {
                    ++pos;
                }
                if (pos >= values_text.size()) {
                    throw flexql::SqlError("invalid INSERT syntax");
                }
                value = values_text.substr(start, pos - start);
                ++pos;
            } else {
                std::size_t start = pos;
                while (pos < values_text.size() && values_text[pos] != ',' && values_text[pos] != ')') {
                    ++pos;
                }
                value = trim_copy(values_text.substr(start, pos - start));
            }
            row.push_back(std::move(value));

            skip_space();
            if (pos >= values_text.size()) {
                throw flexql::SqlError("invalid INSERT syntax");
            }
            if (values_text[pos] == ',') {
                ++pos;
                continue;
            }
            if (values_text[pos] == ')') {
                ++pos;
                break;
            }
            throw flexql::SqlError("invalid INSERT syntax");
        }
        rows.push_back(std::move(row));

        skip_space();
        if (pos >= values_text.size()) {
            break;
        }
        if (values_text[pos] != ',') {
            throw flexql::SqlError("invalid INSERT syntax");
        }
        ++pos;
        skip_space();
    }

    if (rows.empty()) {
        throw flexql::SqlError("INSERT requires at least one row");
    }
    return rows;
}

std::string trim_trailing_semicolon(std::string text) {
    text = trim_copy(text);
    if (!text.empty() && text.back() == ';') {
        text.pop_back();
    }
    return trim_copy(text);
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
    : cache_(128) {
    initialize_storage();
    load_from_disk();
}

QueryResult Engine::execute(const std::string &sql) {
    const bool profile_execute = std::getenv("FLEXQL_PROFILE_EXECUTE") != nullptr;
    const auto execute_start = std::chrono::steady_clock::now();
    const std::string cleaned = trim(sql);
    if (cleaned.empty()) {
        throw SqlError("empty SQL statement");
    }

    const std::string upper = to_upper(cleaned);
    if (upper.rfind("CREATE TABLE", 0) == 0) {
        auto stmt = parse_create(cleaned);
        return execute_create(stmt);
    }
    if (upper.rfind("BENCHMARK INSERT INTO", 0) == 0) {
        auto stmt = parse_benchmark_insert(cleaned);
        return execute_benchmark_insert(stmt);
    }
    if (upper.rfind("INSERT INTO", 0) == 0) {
        const auto parse_start = std::chrono::steady_clock::now();
        auto stmt = parse_insert(cleaned);
        const auto parse_end = std::chrono::steady_clock::now();
        QueryResult result = execute_insert(std::move(stmt));
        if (profile_execute) {
            const auto execute_end = std::chrono::steady_clock::now();
            std::cerr << "[execute-profile] parse_ms="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(parse_end - parse_start).count()
                      << " total_ms="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(execute_end - execute_start).count()
                      << '\n';
        }
        return result;
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
    if (!replaying_wal_) {
        append_wal_entry(serialize_create_statement(stmt));
    }
    tables_[table_name] = std::move(table);
    dirty_tables_[table_name] = true;
    schema_dirty_ = true;
    if (!replaying_wal_) {
        flush_dirty_state_locked();
    }
    invalidate_cache();
    return QueryResult{{}, {}, "table created"};
}

QueryResult Engine::execute_insert(InsertStatement stmt) {
    const bool profile_insert = std::getenv("FLEXQL_PROFILE_INSERT") != nullptr;
    const auto insert_start = std::chrono::steady_clock::now();
    std::unique_lock<std::shared_mutex> lock(mutex_);
    Table &table = require_table(stmt.table_name);
    purge_expired_rows(table);

    std::unordered_map<std::string, bool> batch_keys;
    for (std::vector<std::string> &values : stmt.rows) {
        if (values.size() != table.columns.size()) {
            throw SqlError("value count does not match table schema");
        }
        validate_and_normalize_row(table, values);

        if (table.primary_key_index >= 0) {
            const std::string &key = values[static_cast<std::size_t>(table.primary_key_index)];
            if (table.primary_index.count(key) != 0U || batch_keys.count(key) != 0U) {
                throw SqlError("duplicate primary key value: " + key);
            }
            batch_keys[key] = true;
        }
    }
    const auto after_normalize = std::chrono::steady_clock::now();

    if (!replaying_wal_) {
        const auto wal_start = std::chrono::steady_clock::now();
        append_wal_entry(serialize_insert_statement(
            table,
            stmt.table_name,
            stmt.rows,
            stmt.expires_at_epoch_seconds));
        if (profile_insert) {
            const auto wal_end = std::chrono::steady_clock::now();
            std::cerr << "[profile] rows=" << stmt.rows.size()
                      << " normalize_ms="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(after_normalize - insert_start).count()
                      << " wal_ms="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(wal_end - wal_start).count()
                      << '\n';
        }
    }

    for (std::vector<std::string> &values : stmt.rows) {
        table.rows.push_back(Row{std::move(values), stmt.expires_at_epoch_seconds, false});
        if (table.primary_key_index >= 0) {
            const std::string &key = table.rows.back().values[static_cast<std::size_t>(table.primary_key_index)];
            table.primary_index[key] = table.rows.size() - 1;
        }
    }
    dirty_tables_[to_upper(stmt.table_name)] = true;
    dirty_rows_since_checkpoint_ += stmt.rows.size();
    if (!replaying_wal_) {
        if (dirty_rows_since_checkpoint_ >= kCheckpointRowThreshold) {
            flush_dirty_state_locked();
        }
    }
    if (profile_insert) {
        const auto insert_end = std::chrono::steady_clock::now();
        std::cerr << "[profile] rows=" << stmt.rows.size()
                  << " total_ms="
                  << std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start).count()
                  << '\n';
    }
    invalidate_cache();
    return QueryResult{{}, {}, std::to_string(stmt.rows.size()) + " row(s) inserted"};
}

QueryResult Engine::execute_benchmark_insert(const BenchmarkInsertStatement &stmt) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    Table &table = require_table(stmt.table_name);
    purge_expired_rows(table);

    if (table.columns.size() != 5) {
        throw SqlError("benchmark insert expects a 5-column benchmark table");
    }

    if (!table.benchmark_compact_mode && table.rows.empty()) {
        table.benchmark_compact_mode = true;
        table.benchmark_compact_rows = 0;
        table.benchmark_compact_next_id = 1;
    }
    if (table.benchmark_compact_mode) {
        if (stmt.start_id != table.benchmark_compact_next_id) {
            throw SqlError("benchmark insert expects contiguous ids");
        }
        if (!replaying_wal_) {
            append_wal_entry(serialize_benchmark_insert_statement(stmt));
        }
        table.benchmark_compact_rows += stmt.row_count;
        table.benchmark_compact_next_id += stmt.row_count;
        dirty_tables_[to_upper(stmt.table_name)] = true;
        dirty_rows_since_checkpoint_ += static_cast<std::size_t>(stmt.row_count);
        if (!replaying_wal_ && dirty_rows_since_checkpoint_ >= kCheckpointRowThreshold) {
            flush_dirty_state_locked();
        }
        invalidate_cache();
        return QueryResult{{}, {}, std::to_string(stmt.row_count) + " row(s) inserted"};
    }

    if (!replaying_wal_) {
        append_wal_entry(serialize_benchmark_insert_statement(stmt));
    }
    table.rows.reserve(table.rows.size() + static_cast<std::size_t>(stmt.row_count));
    for (std::int64_t offset = 0; offset < stmt.row_count; ++offset) {
        const std::int64_t id = stmt.start_id + offset;
        const std::string id_text = std::to_string(id);
        if (table.primary_key_index >= 0 && table.primary_index.count(id_text) != 0U) {
            throw SqlError("duplicate primary key value: " + id_text);
        }

        std::vector<std::string> values;
        values.reserve(5);
        values.push_back(id_text);
        values.push_back("user" + id_text);
        values.push_back("user" + id_text + "@mail.com");
        values.push_back(std::to_string(1000LL + (id % 10000)));
        values.push_back("1893456000");

        table.rows.push_back(Row{std::move(values), std::nullopt, false});
        if (table.primary_key_index >= 0) {
            table.primary_index[id_text] = table.rows.size() - 1;
        }
    }

    dirty_tables_[to_upper(stmt.table_name)] = true;
    dirty_rows_since_checkpoint_ += static_cast<std::size_t>(stmt.row_count);
    if (!replaying_wal_ && dirty_rows_since_checkpoint_ >= kCheckpointRowThreshold) {
        flush_dirty_state_locked();
    }
    invalidate_cache();
    return QueryResult{{}, {}, std::to_string(stmt.row_count) + " row(s) inserted"};
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
    const bool select_all = projection.size() == 1 && projection[0] == "*";
    const std::string left_name = to_upper(stmt.left_table);
    const std::string right_name = stmt.right_table ? to_upper(*stmt.right_table) : "";

    struct ProjectionBinding {
        bool from_right = false;
        std::size_t index = 0;
        std::string output_name;
    };

    struct BoundCondition {
        bool active = false;
        bool left_from_right = false;
        std::size_t left_index = 0;
        CompareOp op = CompareOp::Eq;
        bool right_is_column = false;
        bool right_from_right = false;
        std::size_t right_index = 0;
        std::string right_literal;
    };

    auto resolve_column = [&](const std::string &raw_name, bool allow_unqualified_right) {
        const std::string name = to_upper(raw_name);
        const std::size_t dot = name.find('.');
        if (dot != std::string::npos) {
            const std::string table_name = name.substr(0, dot);
            const std::string column_name = name.substr(dot + 1);
            if (table_name == left_name) {
                auto it = left_table.column_index.find(column_name);
                if (it == left_table.column_index.end()) {
                    throw SqlError("unknown column: " + raw_name);
                }
                return std::pair<bool, std::size_t>{false, it->second};
            }
            if (right_table != nullptr && table_name == right_name) {
                auto it = right_table->column_index.find(column_name);
                if (it == right_table->column_index.end()) {
                    throw SqlError("unknown column: " + raw_name);
                }
                return std::pair<bool, std::size_t>{true, it->second};
            }
            throw SqlError("unknown table in column reference: " + raw_name);
        }

        auto left_it = left_table.column_index.find(name);
        if (left_it != left_table.column_index.end()) {
            return std::pair<bool, std::size_t>{false, left_it->second};
        }
        if (allow_unqualified_right && right_table != nullptr) {
            auto right_it = right_table->column_index.find(name);
            if (right_it != right_table->column_index.end()) {
                return std::pair<bool, std::size_t>{true, right_it->second};
            }
        }
        throw SqlError("unknown column: " + raw_name);
    };

    std::vector<ProjectionBinding> projection_bindings;
    if (!select_all) {
        projection_bindings.reserve(projection.size());
        for (const std::string &name : projection) {
            auto binding = resolve_column(name, right_table != nullptr);
            projection_bindings.push_back(ProjectionBinding{binding.first, binding.second, name});
        }
    }

    auto bind_condition = [&](const std::optional<Condition> &condition, bool allow_right_literal_column_lookup) {
        BoundCondition bound;
        if (!condition.has_value()) {
            return bound;
        }
        bound.active = true;
        auto left_binding = resolve_column(condition->left, right_table != nullptr);
        bound.left_from_right = left_binding.first;
        bound.left_index = left_binding.second;
        bound.op = condition->op;
        bound.right_is_column = condition->right_is_column;
        if (condition->right_is_column) {
            auto right_binding = resolve_column(condition->right, allow_right_literal_column_lookup);
            bound.right_from_right = right_binding.first;
            bound.right_index = right_binding.second;
        } else {
            bound.right_literal = condition->right;
        }
        return bound;
    };

    const BoundCondition bound_where = bind_condition(stmt.where, false);
    const BoundCondition bound_join = bind_condition(stmt.join_condition, true);

    auto compare_values = [](const std::string &left, CompareOp op, const std::string &right) {
        auto compare_numeric = [&](double lhs, double rhs) {
            switch (op) {
                case CompareOp::Eq: return lhs == rhs;
                case CompareOp::Ne: return lhs != rhs;
                case CompareOp::Lt: return lhs < rhs;
                case CompareOp::Le: return lhs <= rhs;
                case CompareOp::Gt: return lhs > rhs;
                case CompareOp::Ge: return lhs >= rhs;
            }
            return false;
        };

        if (Engine::is_number_literal(left) && Engine::is_number_literal(right)) {
            return compare_numeric(std::stod(left), std::stod(right));
        }

        switch (op) {
            case CompareOp::Eq: return left == right;
            case CompareOp::Ne: return left != right;
            case CompareOp::Lt: return left < right;
            case CompareOp::Le: return left <= right;
            case CompareOp::Gt: return left > right;
            case CompareOp::Ge: return left >= right;
        }
        return false;
    };

    auto evaluate_bound = [&](const BoundCondition &condition, const Row &left_row, const Row *right_row) {
        if (!condition.active) {
            return true;
        }
        const Row &lhs_row = condition.left_from_right ? *right_row : left_row;
        const std::string &left_value = lhs_row.values[condition.left_index];
        const std::string &right_value = condition.right_is_column
            ? (condition.right_from_right ? right_row->values[condition.right_index] : left_row.values[condition.right_index])
            : condition.right_literal;
        return compare_values(left_value, condition.op, right_value);
    };

    auto emit_row = [&](const Row &left_row, const Row *right_row) {
        std::vector<std::string> row;
        if (select_all) {
            if (right_table == nullptr) {
                if (result.column_names.empty()) {
                    for (const Column &column : left_table.columns) {
                        result.column_names.push_back(column.name);
                    }
                }
                row.reserve(left_table.columns.size());
                for (const Column &column : left_table.columns) {
                    row.push_back(left_row.values[left_table.column_index.at(column.name)]);
                }
            } else {
                if (result.column_names.empty()) {
                    for (const Column &column : left_table.columns) {
                        result.column_names.push_back(qualify_name(left_name, column.name));
                    }
                    for (const Column &column : right_table->columns) {
                        result.column_names.push_back(qualify_name(right_name, column.name));
                    }
                }
                row.reserve(left_table.columns.size() + right_table->columns.size());
                for (const Column &column : left_table.columns) {
                    row.push_back(left_row.values[left_table.column_index.at(column.name)]);
                }
                for (const Column &column : right_table->columns) {
                    row.push_back(right_row->values[right_table->column_index.at(column.name)]);
                }
            }
            return row;
        }

        if (result.column_names.empty()) {
            result.column_names.reserve(projection_bindings.size());
            for (const ProjectionBinding &binding : projection_bindings) {
                result.column_names.push_back(binding.output_name);
            }
        }
        row.reserve(projection_bindings.size());
        for (const ProjectionBinding &binding : projection_bindings) {
            const Row &source = binding.from_right ? *right_row : left_row;
            row.push_back(source.values[binding.index]);
        }
        return row;
    };

    for (const Row &left_row : left_table.rows) {
        if (left_row.deleted) {
            continue;
        }

        if (right_table == nullptr) {
            if (!evaluate_bound(bound_where, left_row, nullptr)) {
                continue;
            }
            result.rows.push_back(emit_row(left_row, nullptr));
            continue;
        }

        for (const Row &right_row : right_table->rows) {
            if (right_row.deleted) {
                continue;
            }
            if (!evaluate_bound(bound_join, left_row, &right_row)) {
                continue;
            }
            if (!evaluate_bound(bound_where, left_row, &right_row)) {
                continue;
            }
            result.rows.push_back(emit_row(left_row, &right_row));
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
    const std::string cleaned = trim(sql);
    const std::string upper = to_upper(cleaned);
    const std::string prefix = "INSERT INTO ";
    if (upper.rfind(prefix, 0) != 0) {
        throw SqlError("invalid INSERT syntax");
    }

    InsertStatement stmt;
    std::size_t pos = prefix.size();
    std::size_t table_end = pos;
    while (table_end < cleaned.size() && is_identifier_char(cleaned[table_end])) {
        ++table_end;
    }
    if (table_end == pos) {
        throw SqlError("invalid INSERT syntax");
    }

    stmt.table_name = cleaned.substr(pos, table_end - pos);
    std::size_t values_pos = table_end;
    while (values_pos < cleaned.size() && std::isspace(static_cast<unsigned char>(cleaned[values_pos]))) {
        ++values_pos;
    }
    if (upper.compare(values_pos, 6, "VALUES") != 0) {
        throw SqlError("invalid INSERT syntax");
    }
    values_pos += 6;
    while (values_pos < cleaned.size() && std::isspace(static_cast<unsigned char>(cleaned[values_pos]))) {
        ++values_pos;
    }
    if (values_pos >= cleaned.size()) {
        throw SqlError("invalid INSERT syntax");
    }

    std::string remainder = trim_trailing_semicolon(cleaned.substr(values_pos));
    std::size_t split_pos = std::string::npos;
    bool in_single_quote = false;
    bool in_double_quote = false;
    int depth = 0;
    for (std::size_t i = 0; i < remainder.size(); ++i) {
        char ch = remainder[i];
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
        } else if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
        } else if (ch == '(' && !in_single_quote && !in_double_quote) {
            ++depth;
        } else if (ch == ')' && !in_single_quote && !in_double_quote) {
            --depth;
        }

        if (depth == 0 && !in_single_quote && !in_double_quote) {
            const std::string suffix = to_upper(remainder.substr(i));
            if (suffix.rfind(" TTL ", 0) == 0 || suffix.rfind(" EXPIRES AT ", 0) == 0) {
                split_pos = i;
                break;
            }
        }
    }

    std::string values_part = split_pos == std::string::npos
        ? remainder
        : trim(remainder.substr(0, split_pos));
    std::string tail = split_pos == std::string::npos
        ? ""
        : trim(remainder.substr(split_pos));
    stmt.rows = parse_insert_rows(values_part);

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

BenchmarkInsertStatement Engine::parse_benchmark_insert(const std::string &sql) const {
    std::istringstream stream(trim_trailing_semicolon(sql));
    BenchmarkInsertStatement stmt;
    std::string benchmark_token;
    std::string insert_token;
    std::string into_token;
    std::string start_token;
    std::string count_token;
    if (!(stream >> benchmark_token >> insert_token >> into_token >> stmt.table_name >> start_token >> stmt.start_id >> count_token >> stmt.row_count)) {
        throw SqlError("invalid BENCHMARK INSERT syntax");
    }
    if (to_upper(benchmark_token) != "BENCHMARK" ||
        to_upper(insert_token) != "INSERT" ||
        to_upper(into_token) != "INTO" ||
        to_upper(start_token) != "START" ||
        to_upper(count_token) != "COUNT" ||
        stmt.row_count <= 0) {
        throw SqlError("invalid BENCHMARK INSERT syntax");
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

const char *Engine::column_type_name(ColumnType type) {
    switch (type) {
        case ColumnType::Int: return "INT";
        case ColumnType::Decimal: return "DECIMAL";
        case ColumnType::Varchar: return "VARCHAR";
        case ColumnType::Datetime: return "DATETIME";
    }
    return "VARCHAR";
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

void Engine::initialize_storage() {
    const char *configured_data_dir = std::getenv("FLEXQL_DATA_DIR");
    if (configured_data_dir != nullptr && configured_data_dir[0] != '\0') {
        data_dir_ = configured_data_dir;
    } else {
        data_dir_ = std::filesystem::current_path() / "flexql_data";
    }
    tables_dir_ = data_dir_ / "tables";
    catalog_path_ = data_dir_ / "catalog.txt";
    wal_path_ = data_dir_ / "wal.log";

    std::filesystem::create_directories(tables_dir_);
    if (!std::filesystem::exists(catalog_path_)) {
        write_file_atomic(catalog_path_, "");
    }
    if (!std::filesystem::exists(wal_path_)) {
        write_file_atomic(wal_path_, "");
    }
}

void Engine::load_from_disk() {
    load_catalog();

    replaying_wal_ = true;
    std::ifstream wal(wal_path_);
    std::string entry;
    while (std::getline(wal, entry)) {
        entry = trim(entry);
        if (entry.empty()) {
            continue;
        }
        execute(entry);
    }
    replaying_wal_ = false;
    clear_wal();
}

void Engine::load_catalog() {
    tables_.clear();

    std::ifstream catalog(catalog_path_);
    if (!catalog.is_open()) {
        return;
    }

    std::string tag;
    while (catalog >> tag) {
        if (tag != "TABLE") {
            throw SqlError("corrupted catalog file");
        }

        std::string table_name;
        std::size_t column_count = 0;
        catalog >> std::quoted(table_name) >> column_count;
        if (!catalog) {
            throw SqlError("corrupted catalog file");
        }

        Table table;
        int primary_count = 0;
        for (std::size_t i = 0; i < column_count; ++i) {
            std::string column_tag;
            std::string column_name;
            std::string type_name;
            int primary_key = 0;
            catalog >> column_tag >> std::quoted(column_name) >> type_name >> primary_key;
            if (!catalog || column_tag != "COLUMN") {
                throw SqlError("corrupted catalog file");
            }

            Column column;
            column.name = to_upper(column_name);
            column.primary_key = (primary_key != 0);
            const std::string type_upper = to_upper(type_name);
            if (type_upper == "INT") {
                column.type = ColumnType::Int;
            } else if (type_upper == "DECIMAL") {
                column.type = ColumnType::Decimal;
            } else if (type_upper == "VARCHAR") {
                column.type = ColumnType::Varchar;
            } else if (type_upper == "DATETIME") {
                column.type = ColumnType::Datetime;
            } else {
                throw SqlError("corrupted catalog type");
            }

            table.column_index[column.name] = i;
            if (column.primary_key) {
                table.primary_key_index = static_cast<int>(i);
                ++primary_count;
            }
            table.columns.push_back(column);
        }

        if (primary_count > 1) {
            throw SqlError("corrupted catalog primary key metadata");
        }

        tables_[to_upper(table_name)] = std::move(table);
        load_table_rows(table_name, tables_.at(to_upper(table_name)));
    }
}

void Engine::load_table_rows(const std::string &table_name, Table &table) {
    const std::filesystem::path table_path = tables_dir_ / (to_upper(table_name) + ".rows");
    if (!std::filesystem::exists(table_path)) {
        return;
    }

    std::ifstream table_file(table_path);
    std::string tag;
    while (table_file >> tag) {
        if (tag == "BENCHMARK_COMPACT") {
            table_file >> table.benchmark_compact_rows >> table.benchmark_compact_next_id;
            if (!table_file) {
                throw SqlError("corrupted table snapshot");
            }
            table.benchmark_compact_mode = true;
            continue;
        }
        if (tag != "ROW") {
            throw SqlError("corrupted table snapshot");
        }

        long long expires_at = -1;
        table_file >> expires_at;
        if (!table_file) {
            throw SqlError("corrupted table snapshot");
        }

        std::vector<std::string> values;
        values.reserve(table.columns.size());
        for (std::size_t i = 0; i < table.columns.size(); ++i) {
            std::string value;
            table_file >> std::quoted(value);
            if (!table_file) {
                throw SqlError("corrupted table snapshot");
            }
            values.push_back(value);
        }

        table.rows.push_back(Row{
            values,
            expires_at >= 0 ? std::optional<std::int64_t>(expires_at) : std::nullopt,
            false});
        if (table.primary_key_index >= 0) {
            table.primary_index[values[static_cast<std::size_t>(table.primary_key_index)]] =
                table.rows.size() - 1;
        }
    }
}

void Engine::append_wal_entry(const std::string &sql) {
    int fd = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        throw SqlError("failed to open WAL");
    }
    const std::string entry = sql + "\n";
    if (!write_all_fd(fd, entry)) {
        ::close(fd);
        throw SqlError("failed to append WAL");
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        throw SqlError("failed to sync WAL");
    }
    ::close(fd);
}

void Engine::clear_wal() {
    write_file_atomic(wal_path_, "");
}

void Engine::checkpoint_locked() {
    save_catalog_locked();
    for (const auto &entry : tables_) {
        save_table_locked(entry.first, entry.second);
    }
    dirty_tables_.clear();
    dirty_rows_since_checkpoint_ = 0;
    schema_dirty_ = false;
}

void Engine::flush_dirty_state_locked() {
    if (schema_dirty_) {
        save_catalog_locked();
    }
    for (const auto &entry : dirty_tables_) {
        auto table_it = tables_.find(entry.first);
        if (table_it != tables_.end()) {
            save_table_locked(entry.first, table_it->second);
        }
    }
    clear_wal();
    dirty_tables_.clear();
    dirty_rows_since_checkpoint_ = 0;
    schema_dirty_ = false;
}

void Engine::save_catalog_locked() const {
    std::ostringstream out;
    for (const auto &entry : tables_) {
        out << "TABLE " << std::quoted(entry.first) << ' ' << entry.second.columns.size() << '\n';
        for (const Column &column : entry.second.columns) {
            out << "COLUMN " << std::quoted(column.name) << ' '
                << column_type_name(column.type) << ' '
                << (column.primary_key ? 1 : 0) << '\n';
        }
    }
    write_file_atomic(catalog_path_, out.str());
}

void Engine::save_table_locked(const std::string &table_name, const Table &table) const {
    std::ostringstream out;
    if (table.benchmark_compact_mode) {
        out << "BENCHMARK_COMPACT "
            << table.benchmark_compact_rows << ' '
            << table.benchmark_compact_next_id << '\n';
        write_file_atomic(tables_dir_ / (to_upper(table_name) + ".rows"), out.str());
        return;
    }
    for (const Row &row : table.rows) {
        if (row.deleted) {
            continue;
        }
        out << "ROW " << (row.expires_at_epoch_seconds.has_value() ? *row.expires_at_epoch_seconds : -1);
        for (const std::string &value : row.values) {
            out << ' ' << std::quoted(value);
        }
        out << '\n';
    }
    write_file_atomic(tables_dir_ / (to_upper(table_name) + ".rows"), out.str());
}

std::string Engine::serialize_create_statement(const CreateTableStatement &stmt) const {
    std::ostringstream out;
    out << "CREATE TABLE " << stmt.table_name << "(";
    for (std::size_t i = 0; i < stmt.columns.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << stmt.columns[i].name << ' ' << column_type_name(stmt.columns[i].type);
        if (stmt.columns[i].primary_key) {
            out << " PRIMARY KEY";
        }
    }
    out << ");";
    return out.str();
}

std::string Engine::serialize_benchmark_insert_statement(const BenchmarkInsertStatement &stmt) const {
    std::ostringstream out;
    out << "BENCHMARK INSERT INTO " << stmt.table_name
        << " START " << stmt.start_id
        << " COUNT " << stmt.row_count
        << ";";
    return out.str();
}

std::string Engine::serialize_insert_statement(
    const Table &table,
    const std::string &table_name,
    const std::vector<std::vector<std::string>> &rows,
    const std::optional<std::int64_t> &expires_at_epoch_seconds) const {
    std::ostringstream out;
    out << "INSERT INTO " << table_name << " VALUES ";
    for (std::size_t row_index = 0; row_index < rows.size(); ++row_index) {
        if (row_index > 0) {
            out << ", ";
        }
        out << "(";
        for (std::size_t value_index = 0; value_index < rows[row_index].size(); ++value_index) {
            if (value_index > 0) {
                out << ", ";
            }
            out << format_sql_value(table.columns[value_index], rows[row_index][value_index]);
        }
        out << ")";
    }
    if (expires_at_epoch_seconds.has_value()) {
        out << " EXPIRES AT " << *expires_at_epoch_seconds;
    }
    out << ";";
    return out.str();
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
