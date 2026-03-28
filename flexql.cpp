#include "flexql.h"
#include "protocol.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <memory>
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

    if (!flexql::send_frame(db->socket_fd, sql)) {
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
