#include "duralog/wal.hpp"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <atomic>
#include <string>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace
{
  using duralog::CommitPolicy;

  CommitPolicy parse_policy(const std::string &value)
  {
    if (value == "sync")
      return CommitPolicy::SyncEveryWrite;
    if (value == "group")
      return CommitPolicy::GroupCommit;
    if (value == "none")
      return CommitPolicy::NoSync;
    throw std::invalid_argument("policy must be sync, group, or none");
  }

  struct Benchmark
  {
    double writes_per_second;
    double p50_us;
    double p99_us;
  };

  Benchmark benchmark(const std::filesystem::path &path, duralog::WalOptions options, std::size_t writes)
  {
    std::filesystem::remove(path);
    duralog::Wal wal(path, options);
    std::vector<double> latency;
    latency.reserve(writes);
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < writes; ++i)
    {
      const auto before = std::chrono::steady_clock::now();
      wal.append("benchmark-record-" + std::to_string(i));
      const auto after = std::chrono::steady_clock::now();
      latency.push_back(std::chrono::duration<double, std::micro>(after - before).count());
    }
    wal.sync();
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    std::sort(latency.begin(), latency.end());
    const auto percentile = [&latency](double p)
    { return latency[static_cast<std::size_t>(p * (latency.size() - 1))]; };
    return {writes / elapsed, percentile(.50), percentile(.99)};
  }

  [[noreturn]] void writer(const std::filesystem::path &log, duralog::WalOptions options, std::atomic<std::uint64_t> *claimed)
  {
    duralog::Wal wal(log, options);
    for (std::uint64_t i = 0;; ++i)
      claimed->store(wal.append("crash-record-" + std::to_string(i)), std::memory_order_release);
  }

  std::uint64_t replay_in_fresh_process(const std::filesystem::path &log)
  {
    int pipefd[2];
    if (::pipe(pipefd) != 0)
      throw std::system_error(errno, std::generic_category(), "create replay pipe");
    const pid_t child = ::fork();
    if (child < 0)
      throw std::system_error(errno, std::generic_category(), "fork replayer");
    if (child == 0)
    {
      ::close(pipefd[0]);
      const auto count = duralog::Wal::replay(log).records.size();
      const auto bytes = ::write(pipefd[1], &count, sizeof(count));
      ::close(pipefd[1]);
      _exit(bytes == static_cast<ssize_t>(sizeof(count)) ? 0 : 1);
    }
    ::close(pipefd[1]);
    std::uint64_t recovered = 0;
    const auto bytes = ::read(pipefd[0], &recovered, sizeof(recovered));
    ::close(pipefd[0]);
    int status = 0;
    if (::waitpid(child, &status, 0) < 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0 || bytes != static_cast<ssize_t>(sizeof(recovered)))
      throw std::runtime_error("fresh replay process failed");
    return recovered;
  }

  void usage()
  {
    std::cerr << "Usage:\n"
              << "  duralog replay <log> [--truncate]\n"
              << "  duralog benchmark <sync|group|none> [writes] [group-size] [group-ms]\n"
              << "  duralog crash-test <sync|group|none> <trials> <seed> [max-delay-ms] [group-size] [group-ms]\n";
  }
}

int main(int argc, char **argv)
{
  try
  {
    if (argc < 2)
    {
      usage();
      return 2;
    }
    const std::string command = argv[1];
    if (command == "replay")
    {
      if (argc < 3 || argc > 4)
      {
        usage();
        return 2;
      }
      const bool truncate = argc == 4 && std::string(argv[3]) == "--truncate";
      if (argc == 4 && !truncate)
      {
        usage();
        return 2;
      }
      const auto replay = truncate ? duralog::Wal::replay_and_truncate(argv[2]) : duralog::Wal::replay(argv[2]);
      std::cout << "recovered=" << replay.records.size() << " valid_bytes=" << replay.valid_bytes;
      if (replay.needs_truncation)
        std::cout << " stopped_at=" << replay.valid_bytes << " reason=" << replay.stop_reason;
      std::cout << '\n';
      return 0;
    }
    if (command == "benchmark")
    {
      if (argc < 3 || argc > 6)
      {
        usage();
        return 2;
      }
      duralog::WalOptions options;
      options.policy = parse_policy(argv[2]);
      const std::size_t writes = argc >= 4 ? std::stoull(argv[3]) : 10000;
      if (argc >= 5)
        options.group_size = std::stoull(argv[4]);
      if (argc >= 6)
        options.group_interval = std::chrono::milliseconds(std::stoull(argv[5]));
      const auto result = benchmark("duralog-benchmark.wal", options, writes);
      std::cout << std::fixed << std::setprecision(2)
                << "policy=" << duralog::policy_name(options.policy) << " writes_per_sec=" << result.writes_per_second
                << " p50_us=" << result.p50_us << " p99_us=" << result.p99_us << '\n';
      return 0;
    }
    if (command == "crash-test")
    {
      if (argc < 5 || argc > 8)
      {
        usage();
        return 2;
      }
      duralog::WalOptions options;
      options.policy = parse_policy(argv[2]);
      const auto trials = std::stoull(argv[3]);
      std::mt19937_64 random(std::stoull(argv[4]));
      const auto max_delay = argc >= 6 ? std::stoull(argv[5]) : 50;
      if (argc >= 7)
        options.group_size = std::stoull(argv[6]);
      if (argc >= 8)
        options.group_interval = std::chrono::milliseconds(std::stoull(argv[7]));
      std::uint64_t total_recovered = 0;
      std::uint64_t total_lost = 0;
      for (std::uint64_t trial = 0; trial < trials; ++trial)
      {
        const std::filesystem::path log = "duralog-crash-" + std::to_string(trial) + ".wal";
        std::filesystem::remove(log);
        constexpr std::size_t kCounterBytes = sizeof(std::atomic<std::uint64_t>);
        auto *claimed = static_cast<std::atomic<std::uint64_t> *>(::mmap(nullptr, kCounterBytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        if (claimed == MAP_FAILED)
          throw std::system_error(errno, std::generic_category(), "allocate shared append counter");
        new (claimed) std::atomic<std::uint64_t>(0);
        const pid_t child = ::fork();
        if (child < 0)
          throw std::system_error(errno, std::generic_category(), "fork writer");
        if (child == 0)
          writer(log, options, claimed);
        std::uniform_int_distribution<unsigned long long> delay(1, std::max(1ULL, max_delay));
        std::this_thread::sleep_for(std::chrono::milliseconds(delay(random)));
        if (::kill(child, SIGKILL) != 0)
          throw std::system_error(errno, std::generic_category(), "kill writer");
        int status = 0;
        if (::waitpid(child, &status, 0) < 0)
          throw std::system_error(errno, std::generic_category(), "wait writer");
        const auto append_claimed = claimed->load(std::memory_order_acquire);
        const auto replay = duralog::Wal::replay(log);
        const auto recovered = replay_in_fresh_process(log);
        // SIGKILL can land between append() returning and the shared-counter store,
        // so this is a conservative lower bound rather than an exact loss count.
        const auto lost = append_claimed >= recovered ? append_claimed - recovered : 0;
        const auto unobserved_ack = recovered > append_claimed ? recovered - append_claimed : 0;
        total_recovered += recovered;
        total_lost += lost;
        std::cout << "trial=" << trial << " claimed=" << append_claimed << " recovered=" << recovered << " loss_lower_bound=" << lost
                  << " unobserved_ack_race=" << unobserved_ack
                  << " valid_bytes=" << replay.valid_bytes << " torn_tail=" << (replay.needs_truncation ? "yes" : "no") << '\n';
        ::munmap(claimed, kCounterBytes);
      }
      std::cout << "summary policy=" << duralog::policy_name(options.policy) << " trials=" << trials
                << " mean_recovered=" << (trials ? static_cast<double>(total_recovered) / trials : 0.0)
                << " mean_lost=" << (trials ? static_cast<double>(total_lost) / trials : 0.0) << '\n';
      return 0;
    }
    usage();
    return 2;
  }
  catch (const std::exception &error)
  {
    std::cerr << "error: " << error.what() << '\n';
    return 1;
  }
}
