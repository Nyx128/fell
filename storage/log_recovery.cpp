#include "storage/log_recovery.hpp"
#include "storage/segment_reader.hpp"
#include <filesystem>
#include <string>

namespace fell::storage {

  RecoveryResult recover_partition(const std::filesystem::path &partition_dir) {
    std::vector<std::filesystem::path> log_paths;
    for (const auto &e : std::filesystem::directory_iterator(partition_dir)) {
      if (e.path().extension() == ".log") {
        log_paths.emplace_back(e.path());
      }
    }
    if (log_paths.empty())
      return {0, 0, 0, false};

    RecoveryResult rr{};

    std::sort(log_paths.begin(), log_paths.end());
    auto last_path = log_paths.back();
    uint64_t seg = std::stoull(last_path.filename().stem().string());
    SegmentReader reader(last_path);

    std::vector<DiskRecord> records;
    uint64_t end_pos = reader.scan_valid(0, records);

    rr.records_recovered = static_cast<uint32_t>(records.size());
    rr.log_end_position = end_pos;

    if (records.empty()) {
      rr.next_offset = seg;
    } else {
      rr.next_offset = records.back().offset + 1;
    }

    size_t seg_size = std::filesystem::file_size(last_path);
    if (end_pos < seg_size) {
      // corrupted data section found
      std::filesystem::resize_file(last_path, end_pos);
      rr.truncated = true;
    }

    return rr;
  }

} // namespace fell::storage
