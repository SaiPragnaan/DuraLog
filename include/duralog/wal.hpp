#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace duralog
{

  struct Record
  {
    std::uint64_t sequence;
    std::string payload;
    bool operator==(const Record &other) const
    {
      return sequence == other.sequence && payload == other.payload;
    }
  };

  enum class CommitPolicy
  {
    SyncEveryWrite,
    GroupCommit,
    NoSync
  };

  struct WalOptions
  {
    CommitPolicy policy = CommitPolicy::SyncEveryWrite;
    std::size_t group_size = 64;
    std::chrono::milliseconds group_interval{10};
  };

  struct ReplayResult
  {
    std::vector<Record> records;
    std::uint64_t valid_bytes = 0;
    bool needs_truncation = false;
    std::string stop_reason;
  };

  class Wal
  {
  public:
    Wal(const std::filesystem::path &path, WalOptions options = {});
    ~Wal();
    Wal(const Wal &) = delete;
    Wal &operator=(const Wal &) = delete;
    Wal(Wal &&) = delete;
    Wal &operator=(Wal &&) = delete;

    std::uint64_t append(const std::string &payload);
    void sync();
    std::uint64_t appended_count() const noexcept;

    static ReplayResult replay(const std::filesystem::path &path);
    static ReplayResult replay_and_truncate(const std::filesystem::path &path);

  private:
    void sync_if_needed();
    int fd_ = -1;
    WalOptions options_;
    std::uint64_t next_sequence_ = 1;
    std::size_t pending_writes_ = 0;
    std::chrono::steady_clock::time_point last_sync_;
  };

  const char *policy_name(CommitPolicy policy);

} // namespace duralog
