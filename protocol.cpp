#include "protocol.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

namespace flexql {

namespace {

bool write_all(int fd, const void *buffer, std::size_t size) {
    const char *cursor = static_cast<const char *>(buffer);
    while (size > 0) {
        ssize_t written = ::send(fd, cursor, size, 0);
        if (written <= 0) {
            return false;
        }
        cursor += written;
        size -= static_cast<std::size_t>(written);
    }
    return true;
}

bool read_all(int fd, void *buffer, std::size_t size) {
    char *cursor = static_cast<char *>(buffer);
    while (size > 0) {
        ssize_t received = ::recv(fd, cursor, size, 0);
        if (received <= 0) {
            return false;
        }
        cursor += received;
        size -= static_cast<std::size_t>(received);
    }
    return true;
}

}  // namespace

bool send_frame(int fd, const std::string &payload) {
    std::uint32_t network_length = htonl(static_cast<std::uint32_t>(payload.size()));
    return write_all(fd, &network_length, sizeof(network_length)) &&
           write_all(fd, payload.data(), payload.size());
}

std::optional<std::string> recv_frame(int fd) {
    std::uint32_t network_length = 0;
    if (!read_all(fd, &network_length, sizeof(network_length))) {
        return std::nullopt;
    }
    std::uint32_t length = ntohl(network_length);
    std::string payload(length, '\0');
    if (length > 0 && !read_all(fd, payload.data(), length)) {
        return std::nullopt;
    }
    return payload;
}

std::string escape_field(const std::string &field) {
    std::string escaped;
    escaped.reserve(field.size());
    for (char ch : field) {
        if (ch == '\\' || ch == '\t' || ch == '\n') {
            escaped.push_back('\\');
            if (ch == '\t') {
                escaped.push_back('t');
            } else if (ch == '\n') {
                escaped.push_back('n');
            } else {
                escaped.push_back(ch);
            }
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
}

std::vector<std::string> split_fields(const std::string &line) {
    std::vector<std::string> fields;
    std::string current;
    bool escaping = false;
    for (char ch : line) {
        if (escaping) {
            if (ch == 't') {
                current.push_back('\t');
            } else if (ch == 'n') {
                current.push_back('\n');
            } else {
                current.push_back(ch);
            }
            escaping = false;
            continue;
        }
        if (ch == '\\') {
            escaping = true;
            continue;
        }
        if (ch == '\t') {
            fields.push_back(current);
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    fields.push_back(current);
    return fields;
}

std::string serialize_result_header(const QueryResult &result) {
    std::string line = "RESULT";
    for (const std::string &column : result.column_names) {
        line.append("\t").append(escape_field(column));
    }
    return line;
}

}  // namespace flexql
