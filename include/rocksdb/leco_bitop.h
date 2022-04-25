#pragma once
#pragma GCC diagnostic ignored "-Wshift-count-overflow"
#include <boost/iostreams/stream.hpp>
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/cpp_int/serialize.hpp>
#include <boost/multiprecision/gmp.hpp>
typedef boost::multiprecision::mpz_int long_int;
typedef leco_uint256 uint256_t;
typedef __uint128_t uint128_t;
typedef uint256_t leco_t;
namespace ROCKSDB_NAMESPACE {

inline long_int convertToLongInt(std::string letter)
{
  std::string s2 = "";
  long_int result = 0;
  int shift = (letter.size() - 1) * 8;

  // for (auto i = 0; i < letter.size(); i++)
  for (auto x:letter)
  {
    // char x = letter.at(i);
    result += long_int(x) << shift;
    // std::cout<<x<<" "<<long_int(x)<<" "<<(long_int(x)<<shift)<<" "<<result<<std::endl;
    shift -= 8;
  }
  return result;
}

template <typename T>
inline T convertToASCII(std::string& letter)
{
  std::string s2 = "";
  T result = 0;
  int shift = (letter.size() - 1) * 8;

  for (auto x:letter)
  {
    result += T((uint8_t)x) << (uint8_t)shift;
    shift -= 8;
  }
  return result;
}

template <typename T>
inline void convertToASCII_char(const char* letter, int size, T* result)
{
 

  uint8_t num_uint64 = size / 8;
  uint8_t num_uint8 = size % 8;
  for (uint8_t i = 0; i < num_uint64; ++i) {
    uint64_t tmp = reinterpret_cast<const uint64_t*>(letter+ size - (i + 1) * 8)[0];
    //memcpy(&tmp, letter + size - (i + 1) * 8, 8);
    tmp = htobe64(tmp);
    memcpy(reinterpret_cast<char*>(result) + i * 8, &tmp, 8);
  }
  
  if (num_uint8) {
    uint64_t tmp = 0;
    memcpy(&tmp, letter, num_uint8);
    tmp = htobe64(tmp) >> (8 * (8 - num_uint8));
    memcpy(reinterpret_cast<char*>(result) + num_uint64 * 8, &tmp, num_uint8);
  }
  // for (uint8_t i = 0; i < num_uint8; ++i) {
  //   (reinterpret_cast<char*>(result) + num_uint64 * 8)[i] =
  //       letter[size - num_uint64 * 8 - i - 1];
  // }
  // int shift = (size - 1) * 8;

  // for (int i=0;i<size;i++)
  // {
  //   *result += T(letter[i]) << (uint8_t)shift;
  //   shift -= 8;
  // }

}

template <typename T>
inline std::string convertToString(T *record, int str_len, int cutoff)
{
  // std::cout<<sizeof(record)<<std::endl;
  const char * res = reinterpret_cast<const char *>(record);
  std::string val = std::string(res , str_len);
  reverse(val.begin(), val.end());
  //std::string ret(val.rbegin(),val.rend());
  return val.substr(0, cutoff);

}

template <typename T>
inline uint32_t bits_T(T v)
{
  uint32_t r(0);
  int length = sizeof(T) * 8;
  if (length > 255 && v >= ((T)1 << (uint8_t)255))
  {
    v >>= 255;
    //v = v/2;
    r += 256;
  }
  if (length > 127 && v >= ((T)1 << (uint8_t)127))
  {
    v >>= 128;
    r += 128;
  }
  if (length > 63 && v >= ((T)1 << (uint8_t)63))
  {
    v >>= 64;
    r += 64;
  }
  if (length > 31 && v >= ((T)1 << (uint8_t)31))
  {
    v >>= 32;
    r += 32;
  }
  if (length > 15 && v >= ((T)1 << (uint8_t)15))
  {
    v >>= 16;
    r += 16;
  }
  if (length > 7 && v >= ((T)1 << (uint8_t)7))
  {
    v >>= 8;
    r += 8;
  }
  if (length > 3 && v >= ((T)1 << (uint8_t)3))
  {
    v >>= 4;
    r += 4;
  }
  if (length > 1 && v >= ((T)1 << (uint8_t)1))
  {
    v >>= 2;
    r += 2;
  }
  if (v >= (T)1)
  {
    r += 1;
  }
  
  return r;
}



// bit op of string compression
template <typename T>
inline void read_all_fix_string(uint8_t *in, int start_byte, int start_index, int numbers, int l, T slope, T start_key, std::vector<std::string> &result)
{
  uint64_t left = 0;
  T decode = 0;
  uint64_t start = start_byte;
  uint64_t end = 0;
  uint64_t total_bit = l * numbers;
  uint64_t writeind = 0;
  T mask0 = ((T)1 << l) - 1;
  T mask1 = ((T)1<< (l - 1)) - 1;
  end = start + (uint64_t)(total_bit / 8);

  if (total_bit % 8 != 0)
  {
    end++;
  }

  while (start <= end)
  {
    /*
    if(writeind>= numbers){
      break;
    }
    */
    while (left >= l)
    {
      T tmp = decode & mask0;
      T tmpval = tmp & mask1;
      T record_val = 0;
      bool sign = (tmp >> (l - 1)) & 1;

      decode = (decode >> l);
      if(sign){
          record_val = tmpval + (T)(start_key + writeind * slope);
      }
      else{
          record_val = (T)(start_key + writeind * slope) - tmpval;
      }
      
      result[writeind] = convertToString<T>(&record_val);
      writeind++;
      left -= l;
      if (left == 0)
      {
        decode = 0;
      }
      // std::cout<<"decode "<<decode<<"left"<<left<<std::endl;
    }
    decode += ((T)in[start] << left);

    start++;
    left += 8;
  }
}

template <typename T>
void read_bit_fix_string(const uint8_t *in, uint32_t l, uint32_t to_find,uint32_t index_origin, T theta1, T theta0, T *result, uint8_t* ori_length)
{
  uint64_t find_bit = to_find * (l+8);
  uint64_t start_byte = find_bit / 8;
  uint8_t start_bit = find_bit % 8;
  uint64_t occupy = start_bit;
  uint64_t total = 0;

  // decode += (((T)(in[start_byte] >> occupy)) << total);
  // total += (8 - occupy);
  // start_byte++;

  // while (total < l+8)
  // {
  //   decode += ((T)(in[start_byte]) << total);
  //   total += 8 ;
  //   start_byte++;
  // }


  
  T decode = 0;
  memcpy(&decode, in+start_byte, sizeof(T));
  decode >>=(uint8_t)start_bit;
  decode &= (((T)1<<(uint8_t)(l+8))-1);
  // T one = 1;
  // one.left_shift((uint8_t)(l+8) ,*result);
  // decode &= (*result - 1);

  *ori_length = (uint8_t)(decode >> (uint8_t)l); // TODO: maybe right shift?
  bool sign = (decode >> (uint8_t)(l - 1)) & 1;
  T value = (decode & (((T)1 << (uint8_t)(l - 1)) - 1));
  T pred = theta0 + theta1 * index_origin;
  if (sign)
  {
    *result = value + pred;

  }
  else
  {
    *result = pred - value;

  }
}


template <typename T>
uint8_t * write_delta_string(T *in, std::vector<bool>& signvec, uint8_t * string_len, uint8_t *out, uint32_t l, size_t numbers)
{
    T code = 0;
    int occupy = 0;
    uint64_t endbit = ((l+8) * numbers);
    int end = 0;
    int writeind = 0;
    T *tmpin = in;
    int readind = 0;
    if (endbit % 8 == 0)
    {
        end = endbit / 8;
    }
    else
    {
        end = (int)endbit / 8 + 1;
    }
    uint8_t *last = out + end;
    T left_val = 0;

    while (out <= last)
    {
        while (occupy < 8)
        {
            if (tmpin >= in + numbers)
            {
                occupy = 8;
                break;
            }

            bool sign = signvec[readind];
            T tmpnum = tmpin[0];
            T value1 =
                (tmpnum & ((((T)1) << (uint8_t)(l - 1)) - 1)) 
               + (((T)sign) << (uint8_t)(l - 1));
            value1 += ((T)string_len[readind])<<(uint8_t)l;


            code += (value1 << (uint8_t)occupy);
            occupy += (l+8);
            tmpin++;
            readind++;
        } //end while
        while (occupy >= 8)
        {
            left_val = code >> (uint8_t)8;
            //std::cout<<code<<std::endl;
            code = code & ((1 << 8) - 1);
            uint8_t tmp_char = code;
            occupy -= 8;
            out[0] = tmp_char;
            code = left_val;
            //std::cout<< writeind<<std::endl;
            //std::cout<<occupy<<" "<<left_val<<" "<<unsigned(out[0])<<std::endl;
            out++;
        }
    }
    
    int pad = ceil((sizeof(T)*8 - l)/8);
    for (int i = 0; i < pad; i++)
    {
        out[0] = 0;
        out++;
    }
    return out;
}



inline uint8_t *write_delta(long long *in, uint8_t *out, uint8_t l, int numbers) {
  uint64_t code = 0;
  int occupy = 0;
  int endbit = (l * numbers);
  int end = 0;
  long long *tmpin = in;
  if (endbit % 8 == 0) {
    end = endbit / 8;
  } else {
    end = (int)endbit / 8 + 1;
  }
  uint8_t *last = out + end;
  uint64_t left_val = 0;

  while (out <= last) {
    while (occupy < 8) {
      if(tmpin >= in + numbers) {
        occupy = 8;
        break;
      }
      bool sign = 1;
      long long tmpnum = tmpin[0];
      if (tmpnum <= 0) {
        sign = 0;
        tmpnum = -tmpnum;
      }

      uint64_t value1 = ((tmpnum & ((1L << (l - 1)) - 1)) + (sign << (l - 1)));
      code += (value1 << occupy);
      occupy += l;

      tmpin+=1;

    }  // end while
    while (occupy >= 8) {
      left_val = code >> 8;
      // std::cout<<code<<std::endl;
      code = code & ((1L << 8) - 1);
      occupy -= 8;
      out[0] = unsigned((uint8_t)code);
      code = left_val;
      // std::cout<<occupy<<" "<<left_val<<" "<<unsigned(out[0])<<std::endl;
      out++;
    }
  }

  return out;
}

inline void read_all_bit_fix(uint8_t *in, int start_byte, int start_index, int numbers,
                      int l, double slope, double start_key, uint64_t *out) {
  int left = 0;
  uint64_t decode = 0;
  int start = start_byte;
  int end = 0;
  int total_bit = l * numbers;
  int writeind = 0;
  uint64_t mask0 = ((uint64_t)1 << l) - 1;
  uint64_t mask1 = ((uint64_t)1 << (l - 1)) - 1;
  end = start + (int)(total_bit / 8);
  uint64_t *res = out;
  if (total_bit % 8 != 0) {
    end++;
  }

  while (start <= end) {
    while (left >= l) {
      uint64_t tmp = decode & mask0;
      long long tmpval = tmp & mask1;
      if (!(((tmp >> (l - 1)) & 1))) {
        tmpval = -tmpval;
      }
      decode = (decode >> l);
      tmpval += (long long)(start_key + (double)writeind * slope);
      *res = tmpval;
      res++;
      writeind++;
      left -= l;
      if (left == 0) {
        decode = 0;
      }
      // std::cout<<"decode "<<decode<<"left"<<left<<std::endl;
    }
    decode += ((long long)in[start] << left);

    start++;
    left += 8;
  }
}

inline void read_bit_fix_char(const uint8_t *in, int l, int to_find, double slope,
                      double start_key, int start, uint64_t * out) {
  int find_bit = to_find * l;
  int start_byte = start + find_bit / 8;
  int start_bit = find_bit % 8;
  int occupy = start_bit;
  long long decode = 0;
  int total = 0;
  while (total < l) {
    decode += ((uint64_t)(in[start_byte] >> occupy) << total);
    total += (8 - occupy);
    occupy = 0;
    start_byte++;
  }

  decode = decode & (((uint64_t)1 << l) - 1);
  bool sign = (decode & ((uint64_t)1 << (l - 1)));
  long long value = (decode & (((uint64_t)1 << (l - 1)) - 1));
  if (!sign) {
    value = -value;
  }
  // std::cout<<"l: "<<l<<" value: "<<value<<" predict: "<< (long long)
  // (start_key +((float)to_find*slope))<<std::endl;
  *out = value + (long long)(start_key + (double)to_find * slope);
  return;

}

}  // namespace compression