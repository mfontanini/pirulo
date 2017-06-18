// Code taken from libtins: https://github.com/mfontanini/libtins/blob/master/include/tins/endianness.h

#pragma once

#include <stdint.h>
#if defined(__unix__) || (defined(__APPLE__) && defined(__MACH__))
    #include <sys/param.h>
#endif

#if defined(__APPLE__)
    #include <sys/types.h>
    #define PIRULO_IS_LITTLE_ENDIAN (BYTE_ORDER == LITTLE_ENDIAN)
    #define PIRULO_IS_BIG_ENDIAN (BYTE_ORDER == BIG_ENDIAN)
#elif defined(BSD)
    #include <sys/endian.h>
    #define PIRULO_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
    #define PIRULO_IS_BIG_ENDIAN (_BYTE_ORDER == _BIG_ENDIAN)
#elif defined(_WIN32)
    #include <cstdlib>
    #define PIRULO_IS_LITTLE_ENDIAN 1
    #define PIRULO_IS_BIG_ENDIAN 0
#else
    #include <endian.h>
    #define PIRULO_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
    #define PIRULO_IS_BIG_ENDIAN (__BYTE_ORDER == __BIG_ENDIAN)
#endif

// Define macros to swap bytes using compiler intrinsics when possible
#if defined(_MSC_VER)
    #define PIRULO_BYTE_SWAP_16(data) _byteswap_ushort(data)
    #define PIRULO_BYTE_SWAP_32(data) _byteswap_ulong(data)
    #define PIRULO_BYTE_SWAP_64(data) _byteswap_uint64(data)
#elif defined(PIRULO_HAVE_GCC_BUILTIN_SWAP)
    #define PIRULO_BYTE_SWAP_16(data) __builtin_bswap16(data)
    #define PIRULO_BYTE_SWAP_32(data) __builtin_bswap32(data)
    #define PIRULO_BYTE_SWAP_64(data) __builtin_bswap64(data)
#else
    #define PIRULO_NO_BYTE_SWAP_INTRINSICS
#endif

namespace pirulo {
namespace endian {

/** 
 * \brief "Changes" a 8-bit integral value's endianess. This is an
 * identity function.
 *
 * \param data The data to convert.
 */
inline uint8_t do_change_endian(uint8_t data) {
    return data;
}

/** 
 * \brief Changes a 16-bit integral value's endianess.
 *
 * \param data The data to convert.
 */
inline uint16_t do_change_endian(uint16_t data) {
    #ifdef PIRULO_NO_BYTE_SWAP_INTRINSICS
        return ((data & 0xff00) >> 8)  | ((data & 0x00ff) << 8);
    #else
        return PIRULO_BYTE_SWAP_16(data);
    #endif
}

/**
 * \brief Changes a 32-bit integral value's endianess.
 *
 * \param data The data to convert.
 */
inline uint32_t do_change_endian(uint32_t data) {
    #ifdef PIRULO_NO_BYTE_SWAP_INTRINSICS
        return (((data & 0xff000000) >> 24) | ((data & 0x00ff0000) >> 8)  |
               ((data & 0x0000ff00) << 8)  | ((data & 0x000000ff) << 24));
    #else
        return PIRULO_BYTE_SWAP_32(data);
    #endif
}

/**
 * \brief Changes a 64-bit integral value's endianess.
 *
 * \param data The data to convert.
 */
 inline uint64_t do_change_endian(uint64_t data) {
    #ifdef PIRULO_NO_BYTE_SWAP_INTRINSICS
        return (((uint64_t)(do_change_endian((uint32_t)(data & 0xffffffff))) << 32) |
               (do_change_endian(((uint32_t)(data >> 32)))));
    #else
        return PIRULO_BYTE_SWAP_64(data);
    #endif
 }
 
/**
 * \cond
 */

// Helpers to convert
template<typename T>
struct conversion_dispatch_helper {
    static T dispatch(T data) {
        return do_change_endian(data);
    }
};


template<size_t>
struct conversion_dispatcher;

template<>
struct conversion_dispatcher<sizeof(uint8_t)> 
: conversion_dispatch_helper<uint8_t> { };

template<>
struct conversion_dispatcher<sizeof(uint16_t)> 
: conversion_dispatch_helper<uint16_t> { };

template<>
struct conversion_dispatcher<sizeof(uint32_t)> 
: conversion_dispatch_helper<uint32_t> { };

template<>
struct conversion_dispatcher<sizeof(uint64_t)> 
: conversion_dispatch_helper<uint64_t> { };

/**
 * \endcond
 */

/**
 * \brief Changes an integral value's endianess.
 * 
 * This dispatchs to the corresponding function.
 *
 * \param data The data to convert.
 */ 
 template<typename T>
 inline T change_endian(T data) {
     return conversion_dispatcher<sizeof(T)>::dispatch(data);
 }

#if PIRULO_IS_LITTLE_ENDIAN
    /** 
     * \brief Convert any integral type to big endian.
     *
     * \param data The data to convert.
     */
    template<typename T>
    inline T host_to_be(T data) {
        return change_endian(data);
    }
     
    /**
     * \brief Convert any integral type to little endian.
     *
     * On little endian platforms, the parameter is simply returned.
     * 
     * \param data The data to convert.
     */
     template<typename T>
     inline T host_to_le(T data) {
         return data;
     }
     
    /**
     * \brief Convert any big endian value to the host's endianess.
     * 
     * \param data The data to convert.
     */
     template<typename T>
     inline T be_to_host(T data) {
         return change_endian(data);
     }
     
    /**
     * \brief Convert any little endian value to the host's endianess.
     * 
     * \param data The data to convert.
     */
     template<typename T>
     inline T le_to_host(T data) {
         return data;
     }
#elif PIRULO_IS_BIG_ENDIAN
    /** 
     * \brief Convert any integral type to big endian.
     *
     * \param data The data to convert.
     */
    template<typename T>
    inline T host_to_be(T data) {
        return data;
    }
     
    /**
     * \brief Convert any integral type to little endian.
     *
     * On little endian platforms, the parameter is simply returned.
     * 
     * \param data The data to convert.
     */
     template<typename T>
     inline T host_to_le(T data) {
         return change_endian(data);
     }
     
    /**
     * \brief Convert any big endian value to the host's endianess.
     * 
     * \param data The data to convert.
     */
     template<typename T>
     inline T be_to_host(T data) {
         return data;
     }
     
    /**
     * \brief Convert any little endian value to the host's endianess.
     * 
     * \param data The data to convert.
     */
     template<typename T>
     inline T le_to_host(T data) {
         return change_endian(data);
     }
#endif
     
} // endian
} // pirulo
