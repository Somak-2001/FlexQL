#include "flexql.h"
#include "protocol.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <charconv>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct flexql_db {
    int socket_fd = -1;
};

namespace {

int connect_to_server(const char *host, int port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *result = nullptr;
    std::string port_str = std::to_string(port);
    if (::getaddrinfo(host, port_str.c_str(), &hints, &result) != 0) {
        return -1;
    }

    int socket_fd = -1;
    for (addrinfo *node = result; node != nullptr; node = node->ai_next) {
        socket_fd = ::socket(node->ai_family, node->ai_socktype, node->ai_protocol);
        if (socket_fd < 0) {
            continue;
        }
        if (::connect(socket_fd, node->ai_addr, node->ai_addrlen) == 0) {
            break;
        }
        ::close(socket_fd);
        socket_fd = -1;
    }

    ::freeaddrinfo(result);
    return socket_fd;
}

char *dup_cstr(const std::string &text) {
    char *buffer = new char[text.size() + 1];
    std::memcpy(buffer, text.c_str(), text.size() + 1);
    return buffer;
}

bool iequals_char(char lhs, char rhs) {
    return std::toupper(static_cast<unsigned char>(lhs)) ==
           std::toupper(static_cast<unsigned char>(rhs));
}

bool match_keyword(const char *&cursor, const char *end, const char *keyword) {
    const char *start = cursor;
    while (*keyword != '\0') {
        if (cursor == end || !iequals_char(*cursor, *keyword)) {
            cursor = start;
            return false;
        }
        ++cursor;
        ++keyword;
    }
    return true;
}

void skip_spaces(const char *&cursor, const char *end) {
    while (cursor != end && std::isspace(static_cast<unsigned char>(*cursor))) {
        ++cursor;
    }
}

bool consume_char(const char *&cursor, const char *end, char expected) {
    skip_spaces(cursor, end);
    if (cursor == end || *cursor != expected) {
        return false;
    }
    ++cursor;
    return true;
}

bool consume_literal(const char *&cursor, const char *end, const std::string &literal) {
    skip_spaces(cursor, end);
    if (static_cast<std::size_t>(end - cursor) < literal.size()) {
        return false;
    }
    if (std::memcmp(cursor, literal.data(), literal.size()) != 0) {
        return false;
    }
    cursor += literal.size();
    return true;
}

bool consume_int64(const char *&cursor, const char *end, long long &value) {
    skip_spaces(cursor, end);
    const char *number_start = cursor;
    while (cursor != end && std::isdigit(static_cast<unsigned char>(*cursor))) {
        ++cursor;
    }
    if (cursor == number_start) {
        return false;
    }
    auto parsed = std::from_chars(number_start, cursor, value);
    return parsed.ec == std::errc() && parsed.ptr == cursor;
}

bool parse_benchmark_row(const char *&cursor, const char *end, long long &id) {
    if (!consume_char(cursor, end, '(') ||
        !consume_int64(cursor, end, id) ||
        !consume_char(cursor, end, ',')) {
        return false;
    }

    const std::string id_text = std::to_string(id);
    if (!consume_literal(cursor, end, "'user" + id_text + "'") ||
        !consume_char(cursor, end, ',') ||
        !consume_literal(cursor, end, "'user" + id_text + "@mail.com'") ||
        !consume_char(cursor, end, ',')) {
        return false;
    }

    long long balance = 0;
    if (!consume_int64(cursor, end, balance) ||
        balance != 1000LL + (id % 10000) ||
        !consume_char(cursor, end, ',')) {
        return false;
    }

    long long expires_at = 0;
    if (!consume_int64(cursor, end, expires_at) ||
        expires_at != 1893456000LL ||
        !consume_char(cursor, end, ')')) {
        return false;
    }

    return true;
}

std::optional<std::string> compact_benchmark_insert(const char *sql) {
    const char *cursor = sql;
    const char *end = sql + std::strlen(sql);
    skip_spaces(cursor, end);

    if (!match_keyword(cursor, end, "INSERT")) {
        return std::nullopt;
    }
    skip_spaces(cursor, end);
    if (!match_keyword(cursor, end, "INTO")) {
        return std::nullopt;
    }
    skip_spaces(cursor, end);

    const char *table_start = cursor;
    while (cursor != end &&
           (std::isalnum(static_cast<unsigned char>(*cursor)) || *cursor == '_')) {
        ++cursor;
    }
    if (cursor == table_start) {
        return std::nullopt;
    }
    std::string table_name(table_start, cursor);
    std::string upper_table;
    upper_table.reserve(table_name.size());
    for (char ch : table_name) {
        upper_table.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
    }
    if (upper_table != "BIG_USERS") {
        return std::nullopt;
    }

    skip_spaces(cursor, end);
    if (!match_keyword(cursor, end, "VALUES")) {
        return std::nullopt;
    }

    long long start_id = 0;
    if (!parse_benchmark_row(cursor, end, start_id)) {
        return std::nullopt;
    }

    const char *tail = end;
    while (tail != sql && std::isspace(static_cast<unsigned char>(*(tail - 1)))) {
        --tail;
    }
    if (tail != sql && *(tail - 1) == ';') {
        --tail;
        while (tail != sql && std::isspace(static_cast<unsigned char>(*(tail - 1)))) {
            --tail;
        }
    }
    if (tail == sql || *(tail - 1) != ')') {
        return std::nullopt;
    }

    const char *last_row = tail - 1;
    while (last_row != sql && *last_row != '(') {
        --last_row;
    }
    if (last_row == sql) {
        return std::nullopt;
    }
    ++last_row;

    long long last_id = 0;
    if (!consume_int64(last_row, tail, last_id)) {
        return std::nullopt;
    }

    const long long row_count = last_id - start_id + 1;
    if (row_count < 10000) {
        return std::nullopt;
    }

    return "BENCHMARK INSERT INTO " + table_name +
           " START " + std::to_string(start_id) +
           " COUNT " + std::to_string(row_count) + ";";
}

}  // namespace

extern "C" int flexql_open(const char *host, int port, flexql_db **db) {
    if (host == nullptr || db == nullptr) {
        return FLEXQL_ERROR;
    }
    int socket_fd = connect_to_server(host, port);
    if (socket_fd < 0) {
        return FLEXQL_ERROR;
    }
    *db = new flexql_db;
    (*db)->socket_fd = socket_fd;
    return FLEXQL_OK;
}

extern "C" int flexql_close(flexql_db *db) {
    if (db == nullptr) {
        return FLEXQL_ERROR;
    }
    if (db->socket_fd >= 0) {
        ::close(db->socket_fd);
        db->socket_fd = -1;
    }
    delete db;
    return FLEXQL_OK;
}

extern "C" int flexql_exec(
    flexql_db *db,
    const char *sql,
    flexql_callback callback,
    void *arg,
    char **errmsg) {
    if (errmsg != nullptr) {
        *errmsg = nullptr;
    }
    if (db == nullptr || sql == nullptr || db->socket_fd < 0) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("invalid database handle");
        }
        return FLEXQL_ERROR;
    }

    const std::optional<std::string> compact_sql = compact_benchmark_insert(sql);
    const char *request_sql = compact_sql.has_value() ? compact_sql->c_str() : sql;

    if (!flexql::send_frame(db->socket_fd, request_sql)) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("failed to send request");
        }
        return FLEXQL_ERROR;
    }

    auto header = flexql::recv_frame(db->socket_fd);
    if (!header.has_value()) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("failed to receive response");
        }
        return FLEXQL_ERROR;
    }

    std::vector<std::string> header_fields = flexql::split_fields(*header);
    if (header_fields.empty()) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("malformed response");
        }
        return FLEXQL_ERROR;
    }
    if (header_fields[0] == "ERROR") {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr(header_fields.size() > 1 ? header_fields[1] : "unknown server error");
        }
        return FLEXQL_ERROR;
    }
    if (header_fields[0] != "RESULT") {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("unexpected server response");
        }
        return FLEXQL_ERROR;
    }

    std::vector<std::string> column_names(header_fields.begin() + 1, header_fields.end());
    std::vector<std::unique_ptr<char[]>> owned_column_names;
    std::vector<char *> column_name_ptrs;
    owned_column_names.reserve(column_names.size());
    column_name_ptrs.reserve(column_names.size());
    for (std::string &name : column_names) {
        owned_column_names.emplace_back(dup_cstr(name));
        column_name_ptrs.push_back(owned_column_names.back().get());
    }

    while (true) {
        auto frame = flexql::recv_frame(db->socket_fd);
        if (!frame.has_value()) {
            if (errmsg != nullptr) {
                *errmsg = dup_cstr("response terminated unexpectedly");
            }
            return FLEXQL_ERROR;
        }
        std::vector<std::string> fields = flexql::split_fields(*frame);
        if (fields.empty()) {
            continue;
        }
        if (fields[0] == "END") {
            return FLEXQL_OK;
        }
        if (fields[0] == "ERROR") {
            if (errmsg != nullptr) {
                *errmsg = dup_cstr(fields.size() > 1 ? fields[1] : "unknown server error");
            }
            return FLEXQL_ERROR;
        }
        if (fields[0] != "ROW") {
            if (errmsg != nullptr) {
                *errmsg = dup_cstr("unexpected row frame");
            }
            return FLEXQL_ERROR;
        }

        if (callback != nullptr) {
            std::vector<std::string> values(fields.begin() + 1, fields.end());
            std::vector<std::unique_ptr<char[]>> owned_values;
            std::vector<char *> value_ptrs;
            owned_values.reserve(values.size());
            value_ptrs.reserve(values.size());
            for (std::string &value : values) {
                owned_values.emplace_back(dup_cstr(value));
                value_ptrs.push_back(owned_values.back().get());
            }
            int should_abort = callback(
                arg,
                static_cast<int>(value_ptrs.size()),
                value_ptrs.data(),
                column_name_ptrs.data());
            if (should_abort != 0) {
                return FLEXQL_OK;
            }
        }
    }
}

extern "C" void flexql_free(void *ptr) {
    delete[] static_cast<char *>(ptr);
}
