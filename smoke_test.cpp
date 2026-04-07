#include "engine.h"

#include <iostream>
#include <string>

int main() {
    try {
        flexql::Engine engine;

        engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, score DECIMAL);");
        engine.execute("CREATE TABLE teams (id INT PRIMARY KEY, lead VARCHAR);");
        engine.execute("INSERT INTO users VALUES (1, 'Alice', 91.5), (2, 'Bob', 88.0) TTL 60;");
        engine.execute("INSERT INTO teams VALUES (1, 'Riya');");

        auto select_result = engine.execute("SELECT id, name FROM users WHERE score >= 90;");
        if (select_result.rows.size() != 1 || select_result.rows[0][1] != "Alice") {
            std::cerr << "SELECT verification failed\n";
            return 1;
        }

        auto join_result = engine.execute(
            "SELECT USERS.ID, USERS.NAME, TEAMS.LEAD FROM users INNER JOIN teams ON USERS.ID = TEAMS.ID;");
        if (join_result.rows.size() != 1 || join_result.rows[0][2] != "Riya") {
            std::cerr << "JOIN verification failed\n";
            return 1;
        }

        std::cout << "Smoke test passed\n";
        return 0;
    } catch (const std::exception &ex) {
        std::cerr << "Smoke test failed: " << ex.what() << "\n";
        return 1;
    }
}
