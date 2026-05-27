#include "storage/offset_index.hpp"
#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_format.hpp"
#include <filesystem>

namespace fell::storage {

  OffsetIndex::OffsetIndex(const std::filesystem::path &idx_path) {
    file_t fd = platform::open_file_read(idx_path);
    if (fd == INVALID_FILE)
      return;

    size_t fsize = std::filesystem::file_size(idx_path);
    uint64_t num_entries = fsize / sizeof(IndexEntry);
    entries_.resize(num_entries);

    // TODO: perhaps find a way i dont have to iterate through the vector 2 times to fix endianess
    // make it faster? if thats possible

    // Read the entire file in one OS call directly into the vector's backing array
    ssize_t bytes_read = platform::pread_file(fd, entries_.data(), fsize, 0);

    if (bytes_read == static_cast<ssize_t>(fsize)) {
      // The data on disk is big-endian; convert back to host byte order
      for (auto &entry : entries_) {
        entry.offset = platform::be64_to_host(entry.offset);
        entry.file_position = platform::be64_to_host(entry.file_position);
      }
    } else {
      // If we failed to read the full file, just clear what we have.
      // LogRecovery handles cleaning up torn indexes.
      entries_.clear();
    }

    platform::close_file(fd);
  }

  uint64_t OffsetIndex::lookup(uint64_t target) const {
    if (entries_.empty())
      return 0;

    // Binary search for the largest entry with offset <= target
    // std::upper_bound gives first entry > target; step back one.
    auto it = std::upper_bound(entries_.begin(), entries_.end(), target,
                               [](uint64_t val, const IndexEntry &e) { return val < e.offset; });

    if (it == entries_.begin())
      return 0; // target < first entry
    --it;
    return it->file_position;
  }

} // namespace fell::storage
