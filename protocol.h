#ifndef FLEXQL_PROTOCOL_H
#define FLEXQL_PROTOCOL_H

#include "engine.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace flexql {

bool send_frame(int fd, const std::string &payload);
std::optional<std::string> recv_frame(int fd);
std::string escape_field(const std::string &field);
std::vector<std::string> split_fields(const std::string &line);
std::string serialize_result_header(const QueryResult &result);

}  // namespace flexql

#endif
