#include "platform/file.hpp"
#include <cstring>
#include <memory>
#include <system_error>

#ifdef FELL_PLATFORM_WINDOWS

namespace fell::platform {

  file_t open_file_append(const std::filesystem::path &path) {
    HANDLE h = CreateFileW(path.c_str(), FILE_APPEND_DATA, FILE_SHARE_READ, nullptr, OPEN_ALWAYS,
                           FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE)
      throw std::system_error(GetLastError(), std::system_category(), "CreateFileW failed");
    return h;
  }

  file_t open_file_read(const std::filesystem::path &path) {
    HANDLE h = CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                           OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE)
      throw std::system_error(GetLastError(), std::system_category(), "CreateFileW failed");
    return h;
  }

  void close_file(file_t fd) {
    CloseHandle(fd);
  }

  bool write_file_vec(file_t fd, const IOBuffer *buffers, size_t count) {
    size_t total_size = 0;
    for (size_t i = 0; i < count; ++i) {
      total_size += buffers[i].size;
    }

    constexpr size_t kStackSize = 4096;
    uint8_t stack_buf[kStackSize];
    std::unique_ptr<uint8_t[]> heap_buf;
    uint8_t *staging = stack_buf;
    if (total_size > kStackSize) {
      heap_buf = std::make_unique<uint8_t[]>(total_size);
      staging = heap_buf.get();
    }

    size_t offset = 0;
    for (size_t i = 0; i < count; ++i) {
      const uint8_t *ptr = static_cast<const uint8_t *>(buffers[i].data);
      std::memcpy(staging + offset, ptr, buffers[i].size);
      offset += buffers[i].size;
    }

    DWORD bytes_written = 0;
    BOOL result = WriteFile(fd, staging, static_cast<DWORD>(total_size), &bytes_written, nullptr);
    return result != 0 && bytes_written == static_cast<DWORD>(total_size);
  }

  ssize_t pread_file(file_t fd, void *buffer, size_t size, uint64_t offset) {
    OVERLAPPED overlapped{};
    overlapped.Offset = static_cast<DWORD>(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = static_cast<DWORD>((offset >> 32) & 0xFFFFFFFF);

    DWORD bytes_read = 0;
    BOOL result = ReadFile(fd, buffer, static_cast<DWORD>(size), &bytes_read, &overlapped);

    if (!result) {
      DWORD err = GetLastError();
      // EOF can come back as ERROR_HANDLE_EOF on synchronous handles with OVERLAPPED offsets.
      if (err == ERROR_HANDLE_EOF)
        return 0;
      return -1;
    }

    // ReadFile succeeded but read 0 bytes — we are at exact EOF.
    return static_cast<ssize_t>(bytes_read);
  }

  bool flush_file(file_t fd) {
    return FlushFileBuffers(fd) != 0;
  }

  bool truncate_file(file_t fd, uint64_t size) {
    LARGE_INTEGER liSize;
    liSize.QuadPart = size;

    if (!SetFilePointerEx(fd, liSize, nullptr, FILE_BEGIN)) {
      return false;
    }
    return SetEndOfFile(fd) != 0;
  }

} // namespace fell::platform
#endif
