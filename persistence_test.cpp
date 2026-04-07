#include "engine.h"

#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::filesystem::path original_cwd = std::filesystem::current_path();
    const std::filesystem::path temp_root =
        std::filesystem::temp_directory_path() / "flexql_persistence_test_workspace";

    try {
        std::filesystem::remove_all(temp_root);
        std::filesystem::create_directories(temp_root);
        std::filesystem::current_path(temp_root);

        {
            flexql::Engine engine;
            engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, score DECIMAL);");
            engine.execute("INSERT INTO users VALUES (1, 'Alice', 91.5), (2, 'Bob', 88.0);");
        }

        {
            flexql::Engine restarted_engine;
            auto result = restarted_engine.execute("SELECT id, name FROM users;");
            if (result.rows.size() != 2) {
                std::cerr << "Expected 2 rows after restart, got " << result.rows.size() << "\n";
                std::filesystem::current_path(original_cwd);
                std::filesystem::remove_all(temp_root);
                return 1;
            }
            if (result.rows[0][0] != "1" || result.rows[0][1] != "Alice" ||
                result.rows[1][0] != "2" || result.rows[1][1] != "Bob") {
                std::cerr << "Recovered rows do not match expected values\n";
                std::filesystem::current_path(original_cwd);
                std::filesystem::remove_all(temp_root);
                return 1;
            }
        }

        std::filesystem::current_path(original_cwd);
        std::filesystem::remove_all(temp_root);
        std::cout << "Persistence test passed\n";
        return 0;
    } catch (const std::exception &ex) {
        std::filesystem::current_path(original_cwd);
        std::filesystem::remove_all(temp_root);
        std::cerr << "Persistence test failed: " << ex.what() << "\n";
        return 1;
    }
}
