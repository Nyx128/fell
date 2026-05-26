#pragma once
#include <cstdint>
#include <filesystem>

#ifdef FELL_PLATFORM_WINDOWS
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

using file_t = HANDLE;
inline const file_t INVALID_FILE = INVALID_HANDLE_VALUE;
#else
#include <fcntl.h>
#include <unistd.h>

using file_t = int;
constexpr file_t INVALID_FILE = -1;
#endif

#include <cstddef>
#include <type_traits>

#ifdef FELL_PLATFORM_WINDOWS
using ssize_t = std::make_signed_t<size_t>;
#else
#include <sys/types.h>
#endif

namespace fell::platform {

  struct IOBuffer {
    const void *data;
    size_t size;
  };

  // Opens a file for appending. Creates it if it doesn't exist.
  // Enforces strict owner-only permissions.
  file_t open_file_append(const std::filesystem::path &path);

  // Opens an existing file for reading only.
  file_t open_file_read(const std::filesystem::path &path);

  // Closes the file handle.
  void close_file(file_t fd);

  // Writes multiple buffers in a single atomic OS-level operation.
  // Returns true on success, false on failure.
  bool write_file_vec(file_t fd, const IOBuffer *buffers, size_t count);

  // Reads data from a specific offset without modifying a global file pointer.
  // Returns the number of bytes read, or -1 on error.
  ssize_t pread_file(file_t fd, void *buffer, size_t size, uint64_t offset);

  // Flush dirty pages to storage.
  // fd: an open file descriptor (POSIX int or Windows HANDLE resolved by file_t).
  // Returns true on success.
  bool flush_file(file_t fd);

  // Portable truncate.
  bool truncate_file(file_t fd, uint64_t size);

} // namespace fell::platform
