#ifndef LECO_UINT256_H
#define LECO_UINT256_H
#pragma once
#include <stdint.h>
#pragma GCC diagnostic ignored "-Wtype-limits"
#include <type_traits>
const __uint128_t leco_uint128_0(0);
const __uint128_t leco_uint128_1(1);
const __uint128_t leco_uint128_64(64);
const __uint128_t leco_uint128_128(128);
const __uint128_t leco_uint128_256(256);

namespace std {
template <>
struct is_arithmetic<__uint128_t> : std::true_type {};
template <>
struct is_integral<__uint128_t> : std::true_type {};
template <>
struct is_unsigned<__uint128_t> : std::true_type {};
}  // namespace std

class leco_uint256 {
 public:
  // Constructor
  // leco_uint256() = default;
  leco_uint256(const leco_uint256& rhs) = default;
  leco_uint256(leco_uint256&& rhs) = default;
  leco_uint256() {
    UPPER = 0;
    LOWER = 0;
  }
  leco_uint256(__uint128_t upper, __uint128_t lower) {
    UPPER = upper;
    LOWER = lower;
  }
  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  leco_uint256(const T& rhs) : LOWER(rhs), UPPER(0) {
    if (std::is_signed<T>::value) {
      if (rhs < 0) {
        UPPER = -1;
      }
    }
  }

  template <
      typename S, typename T,
      typename = typename std::enable_if<
          std::is_integral<S>::value && std::is_integral<T>::value, void>::type>
  leco_uint256(const S& upper_rhs, const T& lower_rhs)
      : LOWER(lower_rhs), UPPER(upper_rhs) {}

  // Comparison Operators
  bool operator==(const __uint128_t& rhs) const;
  bool operator==(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  bool operator==(const T& rhs) const {
    return (!UPPER && (LOWER == __uint128_t(rhs)));
  }

  bool operator!=(const __uint128_t& rhs) const;
  bool operator!=(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  bool operator!=(const T& rhs) const {
    return ((bool)UPPER | (LOWER != __uint128_t(rhs)));
  }

  bool operator>(const __uint128_t& rhs) const;
  bool operator>(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  bool operator>(const T& rhs) const {
    return ((bool)UPPER | (LOWER > __uint128_t(rhs)));
  }

  bool operator<(const __uint128_t& rhs) const;
  bool operator<(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  bool operator<(const T& rhs) const {
    return (!UPPER) ? (LOWER < __uint128_t(rhs)) : false;
  }

  bool operator>=(const __uint128_t& rhs) const;
  bool operator>=(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  bool operator>=(const T& rhs) const {
    return ((*this > rhs) | (*this == rhs));
  }

  bool operator<=(const __uint128_t& rhs) const;
  bool operator<=(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  bool operator<=(const T& rhs) const {
    return ((*this < rhs) | (*this == rhs));
  }

  // Assignment Operator
  leco_uint256& operator=(const leco_uint256& rhs) = default;
  leco_uint256& operator=(leco_uint256&& rhs) = default;
  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256& operator=(const T& rhs) {
    UPPER = leco_uint128_0;

    if (std::is_signed<T>::value) {
      if (rhs < 0) {
        UPPER = -1;
      }
    }

    LOWER = rhs;
    return *this;
  }
  inline leco_uint256& operator=(const bool& rhs) {
    UPPER = 0;
    LOWER = rhs;
    return *this;
  }

  // Arithmetic Operators
  inline leco_uint256 operator+(const leco_uint256& rhs) const {
    return leco_uint256(UPPER + rhs.UPPER + ((LOWER + rhs.LOWER) < LOWER),
                        LOWER + rhs.LOWER);
  }

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256 operator+(const T& rhs) const {
    return leco_uint256(UPPER + ((LOWER + (uint64_t)rhs) < LOWER),
                        LOWER + (uint64_t)rhs);
  }

  inline leco_uint256& operator+=(const leco_uint256& rhs) {
    UPPER += rhs.UPPER + ((LOWER + rhs.LOWER) < LOWER);
    LOWER += rhs.LOWER;
    return *this;
  }

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256& operator+=(const T& rhs) {
    return *this += leco_uint256(rhs);
  }

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256 operator-(const T& rhs) const {
    return leco_uint256(UPPER - ((LOWER - rhs) > LOWER), LOWER - rhs);
  }

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256& operator-=(const T& rhs) {
    return *this = *this - leco_uint256(rhs);
  }

  inline leco_uint256 operator-(const __uint128_t& rhs) const {
    return *this - leco_uint256(rhs);
  }

  inline leco_uint256 operator-(const leco_uint256& rhs) const {
    return leco_uint256(UPPER - rhs.UPPER - ((LOWER - rhs.LOWER) > LOWER),
                        LOWER - rhs.LOWER);
  }

  inline leco_uint256& operator-=(const __uint128_t& rhs) {
    return *this -= leco_uint256(rhs);
  }

  inline leco_uint256& operator-=(const leco_uint256& rhs) {
    *this = *this - rhs;
    return *this;
  }

  // inline leco_uint256 operator*(const leco_uint256 &rhs) const
  // {
  //   // split values into 4 64-bit parts
  //   __uint128_t top[4] = {UPPER >> 64, UPPER & 0xffffffffffffffffUL,
  //                         LOWER >> 64, LOWER & 0xffffffffffffffffUL};
  //   __uint128_t bottom[4] = {rhs.UPPER >> 64, rhs.UPPER &
  //   0xffffffffffffffffUL,
  //                            rhs.LOWER >> 64, rhs.LOWER &
  //                            0xffffffffffffffffUL};
  //   __uint128_t products[4][4];

  //   // multiply each component of the values
  //   for (int y = 3; y > -1; y--)
  //   {
  //     for (int x = 3; x > -1; x--)
  //     {
  //       products[3 - y][x] = top[x] * bottom[y];
  //     }
  //   }

  //   // first row
  //   __uint128_t fourth64 = __uint128_t(products[0][3] &
  //   0xffffffffffffffffUL);
  //   __uint128_t third64 = __uint128_t(products[0][2] & 0xffffffffffffffffUL)
  //   +
  //                         __uint128_t(products[0][3] >> 64);
  //   __uint128_t second64 = __uint128_t(products[0][1] & 0xffffffffffffffffUL)
  //   +
  //                          __uint128_t(products[0][2] >> 64);
  //   __uint128_t first64 = __uint128_t(products[0][0] & 0xffffffffffffffffUL)
  //   +
  //                         __uint128_t(products[0][1] >> 64);

  //   // second row
  //   third64 += __uint128_t(products[1][3] & 0xffffffffffffffffUL);
  //   second64 += __uint128_t(products[1][2] & 0xffffffffffffffffUL) +
  //               __uint128_t(products[1][3] >> 64);
  //   first64 += __uint128_t(products[1][1] & 0xffffffffffffffffUL) +
  //              __uint128_t(products[1][2] >> 64);

  //   // third row
  //   second64 += __uint128_t(products[2][3] & 0xffffffffffffffffUL);
  //   first64 += __uint128_t(products[2][2] & 0xffffffffffffffffUL) +
  //              __uint128_t(products[2][3] >> 64);

  //   // fourth row
  //   first64 += __uint128_t(products[3][3] & 0xffffffffffffffffUL);

  //   // combines the values, taking care of carry over
  //   return leco_uint256(first64 << leco_uint128_64, leco_uint128_0) +
  //          leco_uint256(third64 >> 64, third64 << leco_uint128_64) +
  //          leco_uint256(second64, leco_uint128_0) + leco_uint256(fourth64);
  // }

  // inline leco_uint256 &operator*=(const leco_uint256 &rhs)
  // {
  //   *this = *this * rhs;
  //   return *this;
  // }

  // template <typename T, typename = typename std::enable_if<
  //                           std::is_integral<T>::value, T>::type>
  // inline leco_uint256 operator*(const T &rhs) const
  // {
  //   return *this * leco_uint256(rhs);
  // }

  // template <typename T, typename = typename std::enable_if<
  //                           std::is_integral<T>::value, T>::type>
  // leco_uint256 &operator*=(const T &rhs)
  // {
  //   return *this = *this * leco_uint256(rhs);
  // }

  inline leco_uint256 operator*(const uint32_t& rhs) const {
    // split values into 4 64-bit parts
    // __uint128_t top[4] = {UPPER >> 64, UPPER & 0xffffffffffffffffUL,
    //                       LOWER >> 64, LOWER & 0xffffffffffffffffUL};

    // __uint128_t products[4];

    // // multiply each component of the values
    // for (int x = 3; x > -1; x--) {
    //   products[x] = top[x] * rhs;
    // }

    // combines the values, taking care of carry over
    return leco_uint256(((UPPER >> 64) * rhs) << leco_uint128_64,
                        leco_uint128_0) +
           leco_uint256(((LOWER >> 64) * rhs) >> 64, ((LOWER >> 64) * rhs)
                                                         << leco_uint128_64) +
           leco_uint256(((UPPER & 0xffffffffffffffffUL) * rhs),
                        leco_uint128_0) +
           leco_uint256(((LOWER & 0xffffffffffffffffUL) * rhs));
  }

  inline leco_uint256& operator*=(const uint32_t& rhs) {
    return *this = *this * rhs;
  }

  // Bitwise Operators
  leco_uint256 operator&(const __uint128_t& rhs) const;
  leco_uint256 operator&(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256 operator&(const T& rhs) const {
    return leco_uint256(leco_uint128_0, LOWER & (__uint128_t)rhs);
  }

  leco_uint256& operator&=(const __uint128_t& rhs);
  leco_uint256& operator&=(const leco_uint256& rhs);

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256& operator&=(const T& rhs) {
    UPPER = leco_uint128_0;
    LOWER &= rhs;
    return *this;
  }

  leco_uint256 operator|(const __uint128_t& rhs) const;
  leco_uint256 operator|(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256 operator|(const T& rhs) const {
    return leco_uint256(UPPER, LOWER | __uint128_t(rhs));
  }

  leco_uint256& operator|=(const __uint128_t& rhs);
  leco_uint256& operator|=(const leco_uint256& rhs);

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256& operator|=(const T& rhs) {
    LOWER |= (__uint128_t)rhs;
    return *this;
  }

  leco_uint256 operator^(const __uint128_t& rhs) const;
  leco_uint256 operator^(const leco_uint256& rhs) const;

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256 operator^(const T& rhs) const {
    return leco_uint256(UPPER, LOWER ^ (__uint128_t)rhs);
  }

  leco_uint256& operator^=(const __uint128_t& rhs);
  leco_uint256& operator^=(const leco_uint256& rhs);

  template <typename T, typename = typename std::enable_if<
                            std::is_integral<T>::value, T>::type>
  inline leco_uint256& operator^=(const T& rhs) {
    LOWER ^= (__uint128_t)rhs;
    return *this;
  }

  leco_uint256 operator~() const;

  // Bit Shift Operators
  leco_uint256 operator<<(const uint8_t& shift) const;
  leco_uint256& operator<<=(const uint8_t& shift);
  inline void left_shift(const uint8_t& shift, leco_uint256& result) {
    if (shift < 128) {
      result.UPPER = (UPPER << shift) & (LOWER >> (128 - shift));
      result.LOWER = LOWER << shift;
    } else if ((256 > shift) && (shift > 128)) {
      result.UPPER = LOWER << (shift - leco_uint128_128);
      result.LOWER = leco_uint128_0;
    } else if (shift == 128) {
      result.LOWER = leco_uint128_0;
    } else {
      result = 0;
    }
  }
  // leco_uint256 operator<<(const __uint128_t& shift) const;
  // leco_uint256 operator<<(const leco_uint256& shift) const;

  // template <typename T, typename = typename std::enable_if<
  //                           std::is_integral<T>::value, T>::type>
  // inline leco_uint256 operator<<(const T& rhs) const {
  //   return *this << leco_uint256(rhs);
  // }

  // leco_uint256& operator<<=(const __uint128_t& shift);
  // leco_uint256& operator<<=(const leco_uint256& shift);

  // template <typename T, typename = typename std::enable_if<
  //                           std::is_integral<T>::value, T>::type>
  // inline leco_uint256& operator<<=(const T& rhs) {
  //   *this = *this << leco_uint256(rhs);
  //   return *this;
  // }

  leco_uint256 operator>>(const uint8_t& shift) const;
  leco_uint256& operator>>=(const uint8_t& shift);
  inline void right_shift(const uint8_t& shift, leco_uint256& result) {
    if (shift < 128) {
      result.UPPER = UPPER >> shift;
      result.LOWER = (UPPER << (128 - shift)) & (LOWER >> shift);
    } else if ((256 > shift) && (shift > 128)) {
      result.UPPER = 0;
      result.LOWER = UPPER >> (shift - 128);
    } else if (shift == 128) {
      result.UPPER = 0;
      result.LOWER = UPPER;
    } else {
      result = 0;
    }
  }
  // leco_uint256 operator>>(const __uint128_t& shift) const;
  // leco_uint256 operator>>(const leco_uint256& shift) const;

  // template <typename T, typename = typename std::enable_if<
  //                           std::is_integral<T>::value, T>::type>
  // inline leco_uint256 operator>>(const T& rhs) const {
  //   return *this >> leco_uint256(rhs);
  // }

  // inline leco_uint256& operator>>=(const __uint128_t& shift);
  // inline leco_uint256& operator>>=(const leco_uint256& shift);

  // template <typename T, typename = typename std::enable_if<
  //                           std::is_integral<T>::value, T>::type>
  // inline leco_uint256& operator>>=(const T& rhs) {
  //   *this = *this >> leco_uint256(rhs);
  //   return *this;
  // }

  // Typecast operator
  operator bool() const { return (bool)(UPPER | LOWER); }

  operator uint8_t() const { return (uint8_t)LOWER; }

  operator uint16_t() const { return (uint16_t)LOWER; }

  operator uint32_t() const { return (uint32_t)LOWER; }

  operator uint64_t() const { return (uint64_t)LOWER; }

  // private:
  __uint128_t LOWER, UPPER;
};

#endif  // LECO_UINT256_H