#ifndef FLEXQL_ENGINE_H
#define FLEXQL_ENGINE_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace flexql {

enum class ColumnType {
    Int,
    Decimal,
    Varchar,
    Datetime
};

enum class CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge
};

struct Column {
    std::string name;
    ColumnType type;
    bool primary_key = false;
};

struct Condition {
    std::string left;
    CompareOp op = CompareOp::Eq;
    std::string right;
    bool right_is_column = false;
};

struct CreateTableStatement {
    std::string table_name;
    std::vector<Column> columns;
};

struct InsertStatement {
    std::string table_name;
    std::vector<std::string> values;
    std::optional<std::int64_t> expires_at_epoch_seconds;
};

struct SelectStatement {
    std::vector<std::string> selected_columns;
    std::string left_table;
    std::optional<std::string> right_table;
    std::optional<Condition> join_condition;
    std::optional<Condition> where;
};

struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
    std::string message;
};

class SqlError : public std::runtime_error {
public:
    explicit SqlError(const std::string &msg)
        : std::runtime_error(msg) {}
};

class Engine {
public:
    Engine();

    QueryResult execute(const std::string &sql);

private:
    struct Row {
        std::vector<std::string> values;
        std::optional<std::int64_t> expires_at_epoch_seconds;
        bool deleted = false;
    };

    struct Table {
        std::vector<Column> columns;
        std::unordered_map<std::string, std::size_t> column_index;
        int primary_key_index = -1;
        std::vector<Row> rows;
        std::unordered_map<std::string, std::size_t> primary_index;
    };

    class QueryCache {
    public:
        explicit QueryCache(std::size_t capacity);

        std::optional<QueryResult> get(const std::string &key);
        void put(const std::string &key, const QueryResult &value);
        void clear();

    private:
        using Node = std::pair<std::string, QueryResult>;
        std::size_t capacity_;
        std::list<Node> order_;
        std::unordered_map<std::string, std::list<Node>::iterator> lookup_;
        std::mutex mutex_;
    };

    QueryResult execute_create(const CreateTableStatement &stmt);
    QueryResult execute_insert(const InsertStatement &stmt);
    QueryResult execute_select(const SelectStatement &stmt, const std::string &cache_key);

    CreateTableStatement parse_create(const std::string &sql) const;
    InsertStatement parse_insert(const std::string &sql) const;
    SelectStatement parse_select(const std::string &sql) const;

    static std::string trim(const std::string &input);
    static std::string to_upper(std::string input);
    static std::vector<std::string> split_csv(const std::string &text);
    static std::optional<CompareOp> parse_compare_op(const std::string &token);
    static std::optional<std::int64_t> parse_timestamp_literal(const std::string &text);
    static std::string canonicalize_sql(const std::string &sql);
    static bool is_identifier_char(char ch);
    static bool is_number_literal(const std::string &value);
    static std::string unquote(const std::string &value);

    const Table &require_table(const std::string &table_name) const;
    Table &require_table(const std::string &table_name);
    void validate_and_normalize_row(const Table &table, std::vector<std::string> &values) const;
    void purge_expired_rows(Table &table);
    void invalidate_cache();
    bool evaluate_condition(
        const Condition &condition,
        const std::unordered_map<std::string, std::string> &field_map) const;

    std::unordered_map<std::string, Table> tables_;
    mutable std::shared_mutex mutex_;
    QueryCache cache_;
};

}  // namespace flexql

#endif
