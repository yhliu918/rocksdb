#include "include/rocksdb/leco_uint256.h"
#pragma GCC diagnostic ignored "-Wtype-limits"

// .cpp
const leco_uint256 leco_uint256_0 = 0;
const leco_uint256 leco_uint256_1 = 1;
// namespace std
// { // This is probably not a good idea
//   template <>
//   struct is_arithmetic<leco_uint256> : std::true_type
//   {
//   };
//   template <>
//   struct is_integral<leco_uint256> : std::true_type
//   {
//   };
//   template <>
//   struct is_unsigned<leco_uint256> : std::true_type
//   {
//   };
// } // namespace std

// comparison operators
inline bool leco_uint256::operator==(const __uint128_t& rhs) const {
  return (*this == leco_uint256(rhs));
}

bool leco_uint256::operator==(const leco_uint256& rhs) const {
  return ((UPPER == rhs.UPPER) && (LOWER == rhs.LOWER));
}

inline bool leco_uint256::operator!=(const __uint128_t& rhs) const {
  return (*this != leco_uint256(rhs));
}

inline bool leco_uint256::operator!=(const leco_uint256& rhs) const {
  return ((UPPER != rhs.UPPER) | (LOWER != rhs.LOWER));
}

inline bool leco_uint256::operator>(const __uint128_t& rhs) const {
  return (*this > leco_uint256(rhs));
}

bool leco_uint256::operator>(const leco_uint256& rhs) const {
  if (UPPER == rhs.UPPER) {
    return (LOWER > rhs.LOWER);
  }
  if (UPPER > rhs.UPPER) {
    return true;
  }
  return false;
}

inline bool leco_uint256::operator<(const __uint128_t& rhs) const {
  return (*this < leco_uint256(rhs));
}

bool leco_uint256::operator<(const leco_uint256& rhs) const {
  if (UPPER == rhs.UPPER) {
    return (LOWER < rhs.LOWER);
  }
  if (UPPER < rhs.UPPER) {
    return true;
  }
  return false;
}

inline bool leco_uint256::operator>=(const __uint128_t& rhs) const {
  return (*this >= leco_uint256(rhs));
}

bool leco_uint256::operator>=(const leco_uint256& rhs) const {
  return ((*this > rhs) | (*this == rhs));
}

inline bool leco_uint256::operator<=(const __uint128_t& rhs) const {
  return (*this <= leco_uint256(rhs));
}

bool leco_uint256::operator<=(const leco_uint256& rhs) const {
  return ((*this < rhs) | (*this == rhs));
}

inline leco_uint256 leco_uint256::operator&(const __uint128_t& rhs) const {
  return leco_uint256(leco_uint128_0, LOWER & rhs);
}

leco_uint256 leco_uint256::operator&(const leco_uint256& rhs) const {
  return leco_uint256(UPPER & rhs.UPPER, LOWER & rhs.LOWER);
}

inline leco_uint256& leco_uint256::operator&=(const __uint128_t& rhs) {
  UPPER = leco_uint128_0;
  LOWER &= rhs;
  return *this;
}

leco_uint256& leco_uint256::operator&=(const leco_uint256& rhs) {
  UPPER &= rhs.UPPER;
  LOWER &= rhs.LOWER;
  return *this;
}

inline leco_uint256 leco_uint256::operator|(const __uint128_t& rhs) const {
  return leco_uint256(UPPER, LOWER | rhs);
}

inline leco_uint256 leco_uint256::operator|(const leco_uint256& rhs) const {
  return leco_uint256(UPPER | rhs.UPPER, LOWER | rhs.LOWER);
}

inline leco_uint256& leco_uint256::operator|=(const __uint128_t& rhs) {
  LOWER |= rhs;
  return *this;
}

inline leco_uint256& leco_uint256::operator|=(const leco_uint256& rhs) {
  UPPER |= rhs.UPPER;
  LOWER |= rhs.LOWER;
  return *this;
}

inline leco_uint256 leco_uint256::operator^(const __uint128_t& rhs) const {
  return leco_uint256(UPPER, LOWER ^ rhs);
}

inline leco_uint256 leco_uint256::operator^(const leco_uint256& rhs) const {
  return leco_uint256(UPPER ^ rhs.UPPER, LOWER ^ rhs.LOWER);
}

inline leco_uint256& leco_uint256::operator^=(const __uint128_t& rhs) {
  LOWER ^= rhs;
  return *this;
}

inline leco_uint256& leco_uint256::operator^=(const leco_uint256& rhs) {
  UPPER ^= rhs.UPPER;
  LOWER ^= rhs.LOWER;
  return *this;
}

inline leco_uint256 leco_uint256::operator~() const {
  return leco_uint256(~UPPER, ~LOWER);
}

leco_uint256 leco_uint256::operator<<(const uint8_t& shift) const {
  if (shift == 0) {
    return *this;
  } else if (shift < 128) {
    // this->UPPER = (UPPER << shift) + (LOWER >> (128 - shift));
    // this->LOWER <<= shift;
    // return *this;
    return leco_uint256((UPPER << shift) + (LOWER >> (128 - shift)),
                        LOWER << shift);
  } else if ((256 > shift) && (shift > 128)) {
    return leco_uint256(LOWER << (shift - leco_uint128_128), leco_uint128_0);
  } else if (shift == 128) {
    return leco_uint256(LOWER, leco_uint128_0);
  } else {
    return leco_uint256_0;
    // return *this;
  }
}

leco_uint256& leco_uint256::operator<<=(const uint8_t& shift) {
  if (shift == 0) {
    return *this;
  } else if (shift < 128) {
    UPPER = (UPPER << shift) + (LOWER >> (128 - shift));
    LOWER <<= shift;
  } else if ((256 > shift) && (shift > 128)) {
    UPPER = LOWER << (shift - leco_uint128_128);
    LOWER = leco_uint128_0;
  } else if (shift == 128) {
    UPPER = LOWER << (shift - leco_uint128_128);
    LOWER = leco_uint128_0;
  } else {
    UPPER = 0;
    LOWER = 0;
  }
  return *this;
}

// inline leco_uint256 leco_uint256::operator<<(const __uint128_t& rhs) const {
//   return *this << leco_uint256(rhs);
// }

// leco_uint256 leco_uint256::operator<<(const leco_uint256& rhs) const {
//   const __uint128_t shift = rhs.LOWER;
//   if (((bool)rhs.UPPER) || (shift >= leco_uint128_256)) {
//     return leco_uint256_0;
//   } else if (shift == leco_uint128_128) {
//     return leco_uint256(LOWER, leco_uint128_0);
//   } else if (shift == leco_uint128_0) {
//     return *this;
//   } else if (shift < leco_uint128_128) {
//     return leco_uint256(
//         (UPPER << shift) + (LOWER >> (leco_uint128_128 - shift)),
//         LOWER << shift);
//   } else if ((leco_uint128_256 > shift) && (shift > leco_uint128_128)) {
//     return leco_uint256(LOWER << (shift - leco_uint128_128), leco_uint128_0);
//   } else {
//     return leco_uint256_0;
//   }
// }

// inline leco_uint256& leco_uint256::operator<<=(const __uint128_t& shift) {
//   return *this <<= leco_uint256(shift);
// }

// inline leco_uint256& leco_uint256::operator<<=(const leco_uint256& shift) {
//   *this = *this << shift;
//   return *this;
// }

leco_uint256 leco_uint256::operator>>(const uint8_t& shift) const {
  if (shift == 0) {
    return *this;
  } else if (shift < 128) {
    return leco_uint256(UPPER >> shift,
                        (UPPER << (128 - shift)) + (LOWER >> shift));
  } else if ((256 > shift) && (shift > 128)) {
    return leco_uint256(UPPER >> (shift - 128));
  } else if (shift == 128) {
    return leco_uint256(UPPER);
  } else {
    return leco_uint256_0;
  }
}

leco_uint256& leco_uint256::operator>>=(const uint8_t& shift) {
  if (shift == 0) {
    return *this;
  } else if (shift < 128) {
    LOWER = (UPPER << (128 - shift)) + (LOWER >> shift);
    UPPER >>= shift;
  } else if ((256 > shift) && (shift > 128)) {
    LOWER = UPPER >> (shift - 128);
    UPPER = 0;
  } else if (shift == 128) {
    LOWER = UPPER;
    UPPER = 0;
  } else {
    *this = 0;
  }
  return *this;
}

// inline leco_uint256 leco_uint256::operator>>(const __uint128_t& rhs) const {
//   return *this >> leco_uint256(rhs);
// }

// leco_uint256 leco_uint256::operator>>(const leco_uint256& rhs) const {
//   const __uint128_t shift = rhs.LOWER;
//   if (((bool)rhs.UPPER) | (shift >= leco_uint128_256)) {
//     return leco_uint256_0;
//   } else if (shift == leco_uint128_128) {
//     return leco_uint256(UPPER);
//   } else if (shift == leco_uint128_0) {
//     return *this;
//   } else if (shift < leco_uint128_128) {
//     return leco_uint256(UPPER >> shift, (UPPER << (leco_uint128_128 - shift))
//     +
//                                             (LOWER >> shift));
//   } else if ((leco_uint128_256 > shift) && (shift > leco_uint128_128)) {
//     return leco_uint256(UPPER >> (shift - leco_uint128_128));
//   } else {
//     return leco_uint256_0;
//   }
// }

// inline leco_uint256& leco_uint256::operator>>=(const __uint128_t& shift) {
//   return *this >>= leco_uint256(shift);
// }

// inline leco_uint256& leco_uint256::operator>>=(const leco_uint256& shift) {
//   *this = *this >> shift;
//   return *this;
// }
