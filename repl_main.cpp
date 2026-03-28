#include "flexql.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

struct PrintContext {
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> columns;
};

int collect_rows(void *data, int column_count, char **values, char **column_names) {
    auto *ctx = static_cast<PrintContext *>(data);
    if (ctx->columns.empty()) {
        for (int i = 0; i < column_count; ++i) {
            ctx->columns.emplace_back(column_names[i] ? column_names[i] : "");
        }
    }
    std::vector<std::string> row;
    row.reserve(static_cast<std::size_t>(column_count));
    for (int i = 0; i < column_count; ++i) {
        row.emplace_back(values[i] ? values[i] : "");
    }
    ctx->rows.push_back(std::move(row));
    return 0;
}

void print_table(const PrintContext &ctx) {
    if (ctx.columns.empty()) {
        std::cout << "OK" << std::endl;
        return;
    }

    std::vector<std::size_t> widths(ctx.columns.size(), 0);
    for (std::size_t i = 0; i < ctx.columns.size(); ++i) {
        widths[i] = ctx.columns[i].size();
    }
    for (const auto &row : ctx.rows) {
        for (std::size_t i = 0; i < row.size(); ++i) {
            widths[i] = std::max(widths[i], row[i].size());
        }
    }

    auto print_separator = [&]() {
        std::cout << "+";
        for (std::size_t width : widths) {
            std::cout << std::string(width + 2, '-') << "+";
        }
        std::cout << "\n";
    };

    print_separator();
    std::cout << "|";
    for (std::size_t i = 0; i < ctx.columns.size(); ++i) {
        std::cout << " " << std::left << std::setw(static_cast<int>(widths[i])) << ctx.columns[i] << " |";
    }
    std::cout << "\n";
    print_separator();
    for (const auto &row : ctx.rows) {
        std::cout << "|";
        for (std::size_t i = 0; i < row.size(); ++i) {
            std::cout << " " << std::left << std::setw(static_cast<int>(widths[i])) << row[i] << " |";
        }
        std::cout << "\n";
    }
    print_separator();
    std::cout << ctx.rows.size() << " row(s)\n";
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::stoi(argv[2]) : 9000;

    flexql_db *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL server at " << host << ":" << port << std::endl;
        return 1;
    }

    std::cout << "Connected to FlexQL at " << host << ":" << port << "\n";
    std::cout << "Type SQL statements ending with ';'. Type .exit to quit.\n";

    std::string line;
    std::string sql_buffer;
    while (std::cout << "flexql> " && std::getline(std::cin, line)) {
        if (line == ".exit" || line == ".quit") {
            break;
        }
        sql_buffer.append(line).append("\n");
        if (line.find(';') == std::string::npos) {
            continue;
        }

        PrintContext ctx;
        char *errmsg = nullptr;
        int rc = flexql_exec(db, sql_buffer.c_str(), collect_rows, &ctx, &errmsg);
        if (rc != FLEXQL_OK) {
            std::cerr << "Error: " << (errmsg ? errmsg : "unknown error") << "\n";
            flexql_free(errmsg);
        } else {
            print_table(ctx);
        }
        sql_buffer.clear();
    }

    flexql_close(db);
    return 0;
}
