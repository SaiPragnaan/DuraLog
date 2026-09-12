#include "duralog/wal.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace
{
  std::filesystem::path temporary_log(const char *name)
  {
    return std::filesystem::temp_directory_path() / (std::string("duralog-") + name + "-" + std::to_string(::getpid()) + ".wal");
  }

  void round_trip()
  {
    const auto path = temporary_log("round-trip");
    std::filesystem::remove(path);
    {
      duralog::Wal wal(path, {.policy = duralog::CommitPolicy::SyncEveryWrite});
      assert(wal.append("one") == 1);
      assert(wal.append("two") == 2);
    }
    const auto result = duralog::Wal::replay(path);
    assert(!result.needs_truncation);
    assert((result.records == std::vector<duralog::Record>{{1, "one"}, {2, "two"}}));
    std::filesystem::remove(path);
  }

  void detects_and_repairs_torn_tail()
  {
    const auto path = temporary_log("torn");
    std::filesystem::remove(path);
    {
      duralog::Wal wal(path);
      wal.append("intact");
      wal.append("will-be-truncated");
    }
    const auto full_size = std::filesystem::file_size(path);
    std::filesystem::resize_file(path, full_size - 5);
    const auto result = duralog::Wal::replay(path);
    assert(result.needs_truncation);
    assert(result.stop_reason == "short record body");
    assert(result.records.size() == 1);
    const auto repaired = duralog::Wal::replay_and_truncate(path);
    assert(repaired.needs_truncation);
    assert(std::filesystem::file_size(path) == repaired.valid_bytes);
    const auto after = duralog::Wal::replay(path);
    assert(!after.needs_truncation && after.records.size() == 1);
    std::filesystem::remove(path);
  }

  void detects_checksum_corruption()
  {
    const auto path = temporary_log("checksum");
    std::filesystem::remove(path);
    {
      duralog::Wal wal(path);
      wal.append("payload");
    }
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(16);
    file.put('X');
    file.close();
    const auto result = duralog::Wal::replay(path);
    assert(result.needs_truncation && result.stop_reason == "checksum mismatch");
    assert(result.records.empty());
    std::filesystem::remove(path);
  }
} // namespace

int main()
{
  round_trip();
  detects_and_repairs_torn_tail();
  detects_checksum_corruption();
  std::cout << "all WAL tests passed\n";
}
