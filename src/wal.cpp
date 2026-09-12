#include "duralog/wal.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace duralog
{
  namespace
  {
    constexpr std::uint32_t kMagic = 0x44555241; // "DURA"
    constexpr std::size_t kHeaderBytes = 16;     // magic, length, sequence
    constexpr std::size_t kChecksumBytes = 4;
    constexpr std::uint32_t kMaxPayloadBytes = 64 * 1024 * 1024;

    void put_u32(std::string &out, std::uint32_t value)
    {
      for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<char>(value >> shift));
    }
    void put_u64(std::string &out, std::uint64_t value)
    {
      for (int shift = 56; shift >= 0; shift -= 8)
        out.push_back(static_cast<char>(value >> shift));
    }
    std::uint32_t read_u32(const char *p)
    {
      std::uint32_t value = 0;
      for (int i = 0; i < 4; ++i)
        value = (value << 8) | static_cast<unsigned char>(p[i]);
      return value;
    }
    std::uint64_t read_u64(const char *p)
    {
      std::uint64_t value = 0;
      for (int i = 0; i < 8; ++i)
        value = (value << 8) | static_cast<unsigned char>(p[i]);
      return value;
    }

    // CRC-32 (IEEE): inexpensive, well-understood accidental-corruption detection.
    std::uint32_t crc32(const char *data, std::size_t length)
    {
      std::uint32_t crc = 0xFFFFFFFFU;
      for (std::size_t i = 0; i < length; ++i)
      {
        crc ^= static_cast<unsigned char>(data[i]);
        for (int bit = 0; bit < 8; ++bit)
          crc = (crc >> 1) ^ (0xEDB88320U & -(crc & 1U));
      }
      return ~crc;
    }

    void write_all(int fd, const char *bytes, std::size_t length)
    {
      while (length > 0)
      {
        const auto written = ::write(fd, bytes, length);
        if (written < 0)
        {
          if (errno == EINTR)
            continue;
          throw std::system_error(errno, std::generic_category(), "write WAL");
        }
        bytes += written;
        length -= static_cast<std::size_t>(written);
      }
    }

    std::string read_exact_or_partial(int fd, std::size_t length, bool &eof)
    {
      std::string result(length, '\0');
      std::size_t received = 0;
      eof = false;
      while (received < length)
      {
        const auto read_count = ::read(fd, result.data() + received, length - received);
        if (read_count == 0)
        {
          eof = true;
          break;
        }
        if (read_count < 0)
        {
          if (errno == EINTR)
            continue;
          throw std::system_error(errno, std::generic_category(), "read WAL");
        }
        received += static_cast<std::size_t>(read_count);
      }
      result.resize(received);
      return result;
    }
  } // namespace

  Wal::Wal(const std::filesystem::path &path, WalOptions options) : options_(options), last_sync_(std::chrono::steady_clock::now())
  {
    if (options_.policy == CommitPolicy::GroupCommit && options_.group_size == 0)
      throw std::invalid_argument("group_size must be positive");
    fd_ = ::open(path.c_str(), O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0644);
    if (fd_ < 0)
      throw std::system_error(errno, std::generic_category(), "open WAL");
    const auto existing = replay(path);
    if (existing.needs_truncation)
    {
      ::close(fd_);
      fd_ = -1;
      throw std::runtime_error("refusing to append corrupt WAL; call replay_and_truncate first");
    }
    if (!existing.records.empty())
      next_sequence_ = existing.records.back().sequence + 1;
  }

  Wal::~Wal()
  {
    if (fd_ >= 0)
      ::close(fd_);
  }

  std::uint64_t Wal::append(const std::string &payload)
  {
    if (payload.size() > kMaxPayloadBytes)
      throw std::invalid_argument("payload exceeds 64 MiB");
    const auto sequence = next_sequence_++;
    std::string encoded;
    encoded.reserve(kHeaderBytes + payload.size() + kChecksumBytes);
    put_u32(encoded, kMagic);
    put_u32(encoded, static_cast<std::uint32_t>(payload.size()));
    put_u64(encoded, sequence);
    encoded += payload;
    put_u32(encoded, crc32(encoded.data(), encoded.size()));
    write_all(fd_, encoded.data(), encoded.size());
    ++pending_writes_;
    sync_if_needed();
    return sequence;
  }

  void Wal::sync()
  {
    if (::fdatasync(fd_) != 0)
      throw std::system_error(errno, std::generic_category(), "fdatasync WAL");
    pending_writes_ = 0;
    last_sync_ = std::chrono::steady_clock::now();
  }

  void Wal::sync_if_needed()
  {
    if (options_.policy == CommitPolicy::SyncEveryWrite)
    {
      sync();
      return;
    }
    if (options_.policy == CommitPolicy::GroupCommit)
    {
      const auto elapsed = std::chrono::steady_clock::now() - last_sync_;
      if (pending_writes_ >= options_.group_size || elapsed >= options_.group_interval)
        sync();
    }
  }

  std::uint64_t Wal::appended_count() const noexcept { return next_sequence_ - 1; }

  ReplayResult Wal::replay(const std::filesystem::path &path)
  {
    ReplayResult result;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
    {
      if (errno == ENOENT)
        return result;
      throw std::system_error(errno, std::generic_category(), "open WAL for replay");
    }
    std::uint64_t offset = 0;
    try
    {
      while (true)
      {
        bool eof = false;
        auto header = read_exact_or_partial(fd, kHeaderBytes, eof);
        if (header.empty() && eof)
          break;
        if (header.size() != kHeaderBytes)
        {
          result.needs_truncation = true;
          result.stop_reason = "short record header";
          break;
        }
        const auto magic = read_u32(header.data());
        const auto length = read_u32(header.data() + 4);
        const auto sequence = read_u64(header.data() + 8);
        if (magic != kMagic)
        {
          result.needs_truncation = true;
          result.stop_reason = "bad record magic";
          break;
        }
        if (length > kMaxPayloadBytes)
        {
          result.needs_truncation = true;
          result.stop_reason = "invalid payload length";
          break;
        }
        auto tail = read_exact_or_partial(fd, static_cast<std::size_t>(length) + kChecksumBytes, eof);
        if (tail.size() != static_cast<std::size_t>(length) + kChecksumBytes)
        {
          result.needs_truncation = true;
          result.stop_reason = "short record body";
          break;
        }
        std::string checksummed = header + tail.substr(0, length);
        if (read_u32(tail.data() + length) != crc32(checksummed.data(), checksummed.size()))
        {
          result.needs_truncation = true;
          result.stop_reason = "checksum mismatch";
          break;
        }
        result.records.push_back({sequence, tail.substr(0, length)});
        offset += kHeaderBytes + length + kChecksumBytes;
      }
    }
    catch (...)
    {
      ::close(fd);
      throw;
    }
    ::close(fd);
    result.valid_bytes = offset;
    return result;
  }

  ReplayResult Wal::replay_and_truncate(const std::filesystem::path &path)
  {
    auto result = replay(path);
    if (result.needs_truncation)
    {
      if (::truncate(path.c_str(), static_cast<off_t>(result.valid_bytes)) != 0)
        throw std::system_error(errno, std::generic_category(), "truncate WAL after replay");
    }
    return result;
  }

  const char *policy_name(CommitPolicy policy)
  {
    switch (policy)
    {
    case CommitPolicy::SyncEveryWrite:
      return "sync-every-write";
    case CommitPolicy::GroupCommit:
      return "group-commit";
    case CommitPolicy::NoSync:
      return "no-sync";
    }
    return "unknown";
  }

}
