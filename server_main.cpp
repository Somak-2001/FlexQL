#include "engine.h"
#include "protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

volatile std::sig_atomic_t g_stop = 0;

void signal_handler(int) {
    g_stop = 1;
}

int create_server_socket(int port) {
    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        throw std::runtime_error("failed to create socket");
    }

    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        ::close(server_fd);
        throw std::runtime_error("failed to bind socket");
    }
    if (::listen(server_fd, 64) < 0) {
        ::close(server_fd);
        throw std::runtime_error("failed to listen on socket");
    }
    return server_fd;
}

void handle_client(int client_fd, flexql::Engine &engine) {
    while (true) {
        auto request = flexql::recv_frame(client_fd);
        if (!request.has_value()) {
            break;
        }

        try {
            flexql::QueryResult result = engine.execute(*request);
            if (!flexql::send_frame(client_fd, flexql::serialize_result_header(result))) {
                break;
            }
            for (const auto &row : result.rows) {
                std::string payload = "ROW";
                for (const std::string &value : row) {
                    payload.append("\t").append(flexql::escape_field(value));
                }
                if (!flexql::send_frame(client_fd, payload)) {
                    break;
                }
            }
            if (!flexql::send_frame(client_fd, "END\t" + flexql::escape_field(result.message))) {
                break;
            }
        } catch (const std::exception &ex) {
            if (!flexql::send_frame(client_fd, "ERROR\t" + flexql::escape_field(ex.what()))) {
                break;
            }
        }
    }
    ::close(client_fd);
}

}  // namespace

int main(int argc, char **argv) {
    int port = 9000;
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        int server_fd = create_server_socket(port);
        flexql::Engine engine;
        std::vector<std::thread> workers;

        std::cout << "FlexQL server listening on port " << port << std::endl;
        while (!g_stop) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_fd = ::accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);
            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }
            workers.emplace_back(handle_client, client_fd, std::ref(engine));
        }

        ::close(server_fd);
        for (std::thread &worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    } catch (const std::exception &ex) {
        std::cerr << "Server error: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}
