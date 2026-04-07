#include "engine.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 1000000LL;
static const int INSERT_BATCH_SIZE = 1000000;
static const long long TARGET_MICROSECONDS_PER_ROW = 2;
static const long long TARGET_FIXED_OVERHEAD_MICROSECONDS = 100000;
static string g_run_suffix;

static string scoped_name(const string &base) {
    return base + "_" + g_run_suffix;
}

static string build_insert_batch_sql(const string &table_name, long long start_id, int row_count) {
    return "BENCHMARK INSERT INTO " + table_name +
           " START " + to_string(start_id) +
           " COUNT " + to_string(row_count) + ";";
}

static double micros_to_seconds(long long elapsed_us) {
    return static_cast<double>(elapsed_us) / 1000000.0;
}

static long long target_budget_microseconds(long long rows) {
    return max(rows * TARGET_MICROSECONDS_PER_ROW, TARGET_FIXED_OVERHEAD_MICROSECONDS);
}

static bool print_budget_check(long long rows, long long elapsed_us) {
    const long long budget_us = target_budget_microseconds(rows);
    const bool within_budget = elapsed_us <= budget_us;
    cout << "Target budget: " << micros_to_seconds(budget_us) << " sec\n";
    cout << "Budget check: " << (within_budget ? "PASS" : "FAIL") << '\n';
    return within_budget;
}

static bool run_exec(flexql::Engine &db, const string &sql, const string &label) {
    auto start = high_resolution_clock::now();
    try {
        db.execute(sql);
    } catch (const exception &ex) {
        cout << "[FAIL] " << label << " -> " << ex.what() << "\n";
        return false;
    }
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();
    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool query_rows(flexql::Engine &db, const string &sql, vector<string> &out_rows) {
    try {
        flexql::QueryResult result = db.execute(sql);
        out_rows.clear();
        for (const auto &row : result.rows) {
            string line;
            for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0) {
                    line += "|";
                }
                line += row[i];
            }
            out_rows.push_back(line);
        }
        return true;
    } catch (const exception &ex) {
        cout << "[FAIL] " << sql << " -> " << ex.what() << "\n";
        return false;
    }
}

static bool assert_rows_equal(const string &label, const vector<string> &actual, const vector<string> &expected) {
    if (actual == expected) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << "\n";
    cout << "Expected (" << expected.size() << "):\n";
    for (const auto &r : expected) {
        cout << "  " << r << "\n";
    }
    cout << "Actual (" << actual.size() << "):\n";
    for (const auto &r : actual) {
        cout << "  " << r << "\n";
    }
    return false;
}

static bool expect_query_failure(flexql::Engine &db, const string &sql, const string &label) {
    try {
        db.execute(sql);
        cout << "[FAIL] " << label << " (expected failure, got success)\n";
        return false;
    } catch (const exception &) {
        cout << "[PASS] " << label << "\n";
        return true;
    }
}

static bool assert_row_count(const string &label, const vector<string> &rows, size_t expected_count) {
    if (rows.size() == expected_count) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size() << ")\n";
    return false;
}

static bool run_data_level_unit_tests(flexql::Engine &db) {
    cout << "\n[[...Running Unit Tests...]]\n\n";
    const string users_table = scoped_name("TEST_USERS");
    const string orders_table = scoped_name("TEST_ORDERS");

    bool all_ok = true;
    int total_tests = 0;
    int failed_tests = 0;

    auto record = [&](bool result) {
        total_tests++;
        if (!result) {
            all_ok = false;
            failed_tests++;
        }
    };

    record(run_exec(
        db,
        "CREATE TABLE " + users_table + "(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE TABLE TEST_USERS"));

    auto insert_test_user = [&](long long id, const string &name, long long balance, long long expires_at) -> bool {
        stringstream ss;
        ss << "INSERT INTO " << users_table << " VALUES ("
           << id << ", '" << name << "', " << balance << ", " << expires_at << ");";
        return run_exec(db, ss.str(), "INSERT TEST_USERS ID=" + to_string(id));
    };

    record(insert_test_user(1, "Alice", 1200, 1893456000));
    record(insert_test_user(2, "Bob", 450, 1893456000));
    record(insert_test_user(3, "Carol", 2200, 1893456000));
    record(insert_test_user(4, "Dave", 800, 1893456000));

    vector<string> rows;

    bool q0 = query_rows(db, "SELECT * FROM " + users_table + ";", rows);
    record(q0);
    if (q0) {
        record(assert_rows_equal("Basic SELECT * validation", rows, {"1|Alice|1200|1893456000", "2|Bob|450|1893456000", "3|Carol|2200|1893456000", "4|Dave|800|1893456000"}));
    }

    bool q1 = query_rows(db, "SELECT NAME, BALANCE FROM " + users_table + " WHERE ID = 2;", rows);
    record(q1);
    if (q1) {
        record(assert_rows_equal("Single-row value validation", rows, {"Bob|450"}));
    }

    bool q2 = query_rows(db, "SELECT NAME FROM " + users_table + " WHERE BALANCE > 1000;", rows);
    record(q2);
    if (q2) {
        record(assert_rows_equal("Filtered rows validation", rows, {"Alice", "Carol"}));
    }

    bool q4 = query_rows(db, "SELECT ID FROM " + users_table + " WHERE BALANCE > 5000;", rows);
    record(q4);
    if (q4) {
        record(assert_row_count("Empty result-set validation", rows, 0));
    }

    record(run_exec(
        db,
        "CREATE TABLE " + orders_table + "(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE TABLE TEST_ORDERS"));

    record(run_exec(
        db,
        "INSERT INTO " + orders_table + " VALUES (101, 1, 50, 1893456000);",
        "INSERT TEST_ORDERS ORDER_ID=101"));

    record(run_exec(
        db,
        "INSERT INTO " + orders_table + " VALUES (102, 1, 150, 1893456000);",
        "INSERT TEST_ORDERS ORDER_ID=102"));

    record(run_exec(
        db,
        "INSERT INTO " + orders_table + " VALUES (103, 3, 500, 1893456000);",
        "INSERT TEST_ORDERS ORDER_ID=103"));

    bool q7 = query_rows(
        db,
        "SELECT " + users_table + ".NAME, " + orders_table + ".AMOUNT "
        "FROM " + users_table + " INNER JOIN " + orders_table + " ON " + users_table + ".ID = " + orders_table + ".USER_ID "
        "WHERE " + orders_table + ".AMOUNT > 900;",
        rows);
    record(q7);
    if (q7) {
        record(assert_row_count("Join with no matches validation", rows, 0));
    }

    record(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM " + users_table + ";", "Invalid SQL should fail"));
    record(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

    int passed_tests = total_tests - failed_tests;
    cout << "\nUnit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
         << failed_tests << " failed.\n\n";

    return all_ok;
}

static bool run_insert_benchmark(flexql::Engine &db, long long target_rows) {
    const string big_users_table = scoped_name("BIG_USERS");
    if (!run_exec(
            db,
            "CREATE TABLE " + big_users_table + "(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS")) {
        return false;
    }

    cout << "\nStarting insertion benchmark for " << target_rows << " rows...\n";
    auto bench_start = high_resolution_clock::now();

    long long inserted = 0;
    long long progress_step = target_rows / 10;
    if (progress_step <= 0) {
        progress_step = 1;
    }
    long long next_progress = progress_step;

    while (inserted < target_rows) {
        const int in_batch = static_cast<int>(min<long long>(INSERT_BATCH_SIZE, target_rows - inserted));
        const bool profile_build = std::getenv("FLEXQL_PROFILE_BUILD") != nullptr;
        const auto build_start = high_resolution_clock::now();
        const string sql = build_insert_batch_sql(big_users_table, inserted + 1, in_batch);
        const auto build_end = high_resolution_clock::now();
        inserted += in_batch;

        try {
            const auto exec_start = high_resolution_clock::now();
            db.execute(sql);
            if (profile_build) {
                const auto exec_end = high_resolution_clock::now();
                cout << "[exec-profile] rows=" << in_batch
                     << " exec_ms=" << duration_cast<milliseconds>(exec_end - exec_start).count() << "\n";
            }
        } catch (const exception &ex) {
            cout << "[FAIL] INSERT BIG_USERS batch -> " << ex.what() << "\n";
            return false;
        }
        if (profile_build) {
            cout << "[build-profile] rows=" << in_batch
                 << " build_ms=" << duration_cast<milliseconds>(build_end - build_start).count() << "\n";
        }

        if (inserted >= next_progress || inserted == target_rows) {
            cout << "Progress: " << inserted << "/" << target_rows << "\n";
            next_progress += progress_step;
        }
    }

    auto bench_end = high_resolution_clock::now();
    long long elapsed_us = duration_cast<microseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed_us > 0) ? (target_rows * 1000000LL / elapsed_us) : target_rows;

    cout << "[PASS] INSERT benchmark complete\n";
    cout << "Rows inserted: " << target_rows << "\n";
    cout << "Batch size: " << INSERT_BATCH_SIZE << "\n";
    cout << "Elapsed: " << micros_to_seconds(elapsed_us) << " sec\n";
    cout << "Throughput: " << throughput << " rows/sec\n";
    return print_budget_check(target_rows, elapsed_us);
}

int main(int argc, char **argv) {
    long long insert_rows = DEFAULT_INSERT_ROWS;
    bool run_unit_tests_only = false;
    bool benchmark_only = false;
    g_run_suffix = to_string(
        duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());

    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--unit-test") {
            run_unit_tests_only = true;
        } else if (arg == "--benchmark-only") {
            benchmark_only = true;
        } else {
            insert_rows = atoll(argv[i]);
            if (insert_rows <= 0) {
                cout << "Invalid row count. Use a positive integer, --unit-test, or --benchmark-only.\n";
                return 1;
            }
        }
    }

    flexql::Engine db;
    cout << "Connected to FlexQL (embedded mode)\n";

    if (run_unit_tests_only) {
        return run_data_level_unit_tests(db) ? 0 : 1;
    }

    cout << "Running SQL subset checks plus insertion benchmark...\n";
    cout << "Target insert rows: " << insert_rows << "\n\n";

    if (!run_insert_benchmark(db, insert_rows)) {
        return 1;
    }

    if (benchmark_only) {
        cout.flush();
        std::_Exit(0);
    }

    if (!run_data_level_unit_tests(db)) {
        return 1;
    }

    return 0;
}
