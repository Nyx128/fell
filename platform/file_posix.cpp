#include "platform/file.hpp"

#ifdef FELL_PLATFORM_LINUX
#include <unistd.h>

namespace fell::platform {
  file_t open_file_append(const std::filesystem::path &path) {
    return open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
  }

  file_t open_file_read(const std::filesystem::path &path) {
    return open(path.c_str(), O_RDONLY);
  }

  void close_file(file_t fd) {
    if (fd >= 0) {
      close(fd);
    }
  }

  bool write_file_vec(file_t fd, const IOBuffer *buffers, size_t count) {
    iovec vecs[IOV_MAX];

    if (count > IOV_MAX) {
      return false;
    }

    for (size_t i = 0; i < count; ++i) {
      vecs[i].iov_base = const_cast<void *>(buffers[i].data);

      vecs[i].iov_len = buffers[i].size;
    }

    ssize_t result = writev(fd, vecs, static_cast<int>(count));

    return result >= 0;
  }

  ssize_t pread_file(file_t fd, void *buffer, size_t size, uint64_t offset) {
    return pread(fd, buffer, size, offset);
  }

  bool flush_file(file_t fd) {
    return ::fdatasync(fd) == 0;
  }

  bool truncate_file(file_t fd, uint64_t size) {
    return ::ftruncate(fd, size) == 0;
  }

} // namespace fell::platform
#endif
