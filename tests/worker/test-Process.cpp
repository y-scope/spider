// NOLINTBEGIN(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)

#include <unistd.h>

#include <chrono>
#include <optional>

#include <catch2/catch_test_macros.hpp>

#include "../../src/spider/io/BoostAsio.hpp"  // IWYU pragma: keep
#include "../../src/spider/worker/Process.hpp"

namespace {

TEST_CASE("Process exit", "[worker]") {
    spider::worker::Process const true_process
            = spider::worker::Process::spawn("true", {}, std::nullopt, std::nullopt, std::nullopt);
    REQUIRE(true_process.wait() == 0);
    spider::worker::Process const false_process
            = spider::worker::Process::spawn("false", {}, std::nullopt, std::nullopt, std::nullopt);
    REQUIRE(false_process.wait() == 1);
}

TEST_CASE("Process cancel", "[worker]") {
    spider::worker::Process const sleep_process = spider::worker::Process::spawn(
            "sleep",
            {"10"},
            std::nullopt,
            std::nullopt,
            std::nullopt
    );
    std::chrono::steady_clock::time_point const start = std::chrono::steady_clock::now();
    sleep_process.terminate();
    std::chrono::steady_clock::time_point const end = std::chrono::steady_clock::now();
    REQUIRE(sleep_process.wait() == 128 + 9);
    std::chrono::steady_clock::duration duration = end - start;
    REQUIRE(duration < std::chrono::seconds(10));
}

TEST_CASE("Process pipe", "[worker]") {
    boost::asio::io_context io_context;
    int write_pipe_fd[2];
    int read_pipe_fd[2];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    REQUIRE(0 == pipe(write_pipe_fd));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    REQUIRE(0 == pipe(read_pipe_fd));
    boost::asio::writable_pipe write_pipe(io_context);
    write_pipe.assign(write_pipe_fd[1]);
    boost::asio::readable_pipe read_pipe(io_context);
    read_pipe.assign(read_pipe_fd[0]);
    spider::worker::Process const echo_process = spider::worker::Process::spawn(
            "cat",
            {},
            write_pipe_fd[0],
            read_pipe_fd[1],
            std::nullopt
    );
    std::string const message = "Hello, World!";
    boost::asio::write(write_pipe, boost::asio::buffer(message));
    std::string buffer;
    buffer.resize(message.size());
    boost::asio::read(read_pipe, boost::asio::buffer(buffer));
    REQUIRE(0 == echo_process.wait());
    REQUIRE(buffer == message);
}

}  // namespace

// NOLINTEND(cert-err58-cpp,cppcoreguidelines-avoid-do-while,readability-function-cognitive-complexity,cppcoreguidelines-avoid-non-const-global-variables,cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
