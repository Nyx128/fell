#pragma once
#include <cstdint>

#if defined(FELL_PLATFORM_WINDOWS)
#include <stdlib.h>
#endif

namespace fell::platform {

  inline uint16_t host_to_be16(uint16_t v) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap16(v);
#elif defined(FELL_PLATFORM_WINDOWS)
    return _byteswap_ushort(v);
#else
    return v;
#endif
  }

  inline uint64_t host_to_be64(uint64_t v) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap64(v);
#elif defined(FELL_PLATFORM_WINDOWS)
    return _byteswap_uint64(v);
#else
    return v; // already big-endian
#endif
  }

  inline uint32_t host_to_be32(uint32_t v) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap32(v);
#elif defined(FELL_PLATFORM_WINDOWS)
    return _byteswap_ulong(v);
#else
    return v;
#endif
  }

  inline uint64_t be64_to_host(uint64_t v) {
    return host_to_be64(v);
  }
  inline uint32_t be32_to_host(uint32_t v) {
    return host_to_be32(v);
  }
  inline uint16_t be16_to_host(uint16_t v) {
    return host_to_be16(v);
  }

} // namespace fell::platform
