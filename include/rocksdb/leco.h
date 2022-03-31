#pragma once
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wbool-compare"
#include <limits>

#include "leco_uint256.h"
#include "leco_utils.h"

#define INF 0x7f7fffff
// Leco string compression alg.
namespace ROCKSDB_NAMESPACE {
template <typename T>
class Leco_string {
 public:
  void Padd_one_string(std::string& s, char x) {
    s.append(max_padding_length - s.size(), x);
  }
  void Padding_string(std::vector<std::string>& string_vec, int N, char x) {
    totalsize_without_padding = 0;
    totalsize_with_padding = 0;
    max_padding_length = 0;
    for (int i = 0; i < blocks; i++) {
      int block_length = block_size;
      if (i == blocks - 1) {
        block_length = N - (blocks - 1) * block_size;
      }
      uint8_t max_length = 0;

      for (int j = 0; j < block_length; j++) {
        totalsize_without_padding += string_vec[i * block_size + j].size();
        if (string_vec[i * block_size + j].size() > max_length) {
          max_length = string_vec[i * block_size + j].size();
        }
      }
      if (max_padding_length < max_length) {
        max_padding_length = max_length;
      }
      padding_length.emplace_back(max_length);
      totalsize_with_padding += max_length * block_length;
      for (int j = 0; j < block_length; j++) {
        int temp_len = string_vec[i * block_size + j].size();
        string_wo_padding_length.emplace_back((uint8_t)temp_len);
        std::string tmp_str_max = string_vec[i * block_size + j];
        std::string tmp_str_min = tmp_str_max;
        padding_max.emplace_back(tmp_str_max.append(
            max_length - temp_len, std::numeric_limits<char>::max()));
        padding_min.emplace_back(tmp_str_min.append(max_length - temp_len, 1));
        string_vec[i * block_size + j].append(max_length - temp_len, x);
      }
    }
  }
  uint8_t* encodeArray8_string(std::vector<std::string>& string_vec,
                               int start_idx, const size_t length, uint8_t* res,
                               size_t nvalue) {
    uint8_t* out = res;
    std::vector<T> ascii_vec;
    std::vector<T> ascii_vec_min;
    std::vector<T> ascii_vec_max;
    std::vector<long_int>
        long_int_vec;  // because the calculation in linear regression may
                       // overflow, we choose to adopt long_int in the string_lr
                       // and convert its param to type T later
    std::vector<int> index;
    for (size_t i = 0; i < length; i++) {
      ascii_vec.emplace_back(convertToASCII<T>(string_vec[i + start_idx]));
      ascii_vec_min.emplace_back(convertToASCII<T>(padding_min[i + start_idx]));
      ascii_vec_max.emplace_back(convertToASCII<T>(padding_max[i + start_idx]));
      long_int_vec.emplace_back(convertToLongInt(string_vec[i + start_idx]));
      index.emplace_back(i);
    }

    string_lr mylr;
    mylr.caltheta(index, long_int_vec, length);

    T theta0 = 0;
    // auto theta0_len = (mpz_sizeinbase(mylr.theta0.backend().data(), 2) + 7) /
    // 8;
    mpz_export(&theta0, NULL, -1, 1, 1, 0, mylr.theta0.backend().data());
    // print_u128_u(theta0);
    // printf("\n");

    T theta1 = 0;
    // auto theta1_len = (mpz_sizeinbase(mylr.theta1.backend().data(), 2) + 7) /
    // 8;
    mpz_export(&theta1, NULL, -1, 1, 1, 0, mylr.theta1.backend().data());
    // print_u128_u(theta1);
    // printf("\n");
    // std::cout<<"theta0: "<<mylr.theta0<<std::endl;
    // std::cout<<"theta1: "<<mylr.theta1<<std::endl;

    std::vector<T> delta;
    std::vector<bool> signvec;
    T max_delta = 0;
    for (uint32_t i = 0; i < length; i++) {
      T tmp_val;

      T pred = theta0 + theta1 * i;
      if (pred > ascii_vec_max[i]) {
        tmp_val = pred - ascii_vec_max[i];
        signvec.emplace_back(false);
      } else if (pred < ascii_vec_min[i]) {
        tmp_val = ascii_vec_min[i] - pred;
        signvec.emplace_back(true);
      } else {
        tmp_val = 0;
        signvec.emplace_back(false);
      }
      
      delta.emplace_back(tmp_val);

      if (tmp_val > max_delta) {
        max_delta = tmp_val;
      }
    }

    uint32_t max_bit = 0;
    if (max_delta) {
      max_bit = bits_T(max_delta) + 1;
    }
    // std::cout<< "max_bit: " << max_bit << std::endl;
    memcpy(out, &max_bit, sizeof(uint32_t));
    out += sizeof(uint32_t);
    memcpy(out, &padding_length[nvalue], sizeof(uint8_t));
    out+=sizeof(uint8_t);
    memcpy(out, &theta0, sizeof(T));
    out += sizeof(T);
    memcpy(out, &theta1, sizeof(T));
    out += sizeof(T);
    if (max_bit) {
      out = write_delta_string(delta.data(), signvec,string_wo_padding_length.data()+block_size*nvalue, out, max_bit, length);
    }

    return out;
  }

  void decodeArray8_string(uint8_t* in, const size_t length, size_t nvalue,
                           std::vector<std::string>& string_vec) {
    uint32_t maxbits;
    uint8_t* tmpin = in;
    memcpy(&maxbits, tmpin, sizeof(uint32_t));
    // std::cout<<"max bit "<<maxbits<<std::endl;
    tmpin += sizeof(uint32_t);

    T theta0;
    memcpy(&theta0, tmpin, sizeof(T));
    tmpin += sizeof(T);

    T theta1;
    memcpy(&theta1, tmpin, sizeof(T));
    tmpin += sizeof(T);

    if (maxbits == 0) {
      for (int i = 0; i < length; i++) {
        T tmp_val = theta0 + theta1 * i;
        string_vec.emplace_back(convertToString<T>(&tmp_val));
      }
    } else {
      read_all_fix_string<T>(tmpin, 0, 0, length, maxbits, theta1, theta0,
                             string_vec);
    }
    return;
  }


  bool pre_Bsearch(int left, int right, int* index, std::string& key,
                   bool* skip_linear_scan, int string_len,
                   int search_len) {  // check which block the key belongs to
    while (left != right) {
      // The `mid` is computed by rounding up so it lands in (`left`, `right`].
      int64_t mid = left + (right - left + 1) / 2;
      std::string tmp_key =
          firstkey_each_block.substr(string_len * mid, search_len);
      if (tmp_key < key) {
        // Key at "mid" is smaller than "target". Therefore all
        // blocks before "mid" are uninteresting.
        left = mid;
      } else if (tmp_key > key) {
        // Key at "mid" is >= "target". Therefore all blocks at or
        // after "mid" are uninteresting.
        right = mid - 1;
      } else {
        *skip_linear_scan = true;
        left = right = mid;
      }
    }

    if (left == -1) {
      // All keys in the block were strictly greater than `target`. So the very
      // first key in the block is the final seek result.
      *skip_linear_scan = true;
      *index = 0;
    } else {
      *index = static_cast<uint32_t>(left);
    }
    return true;
  }



  void setBlockSize(int block_size_, int block_num) {
    this->block_size = block_size_;
    this->blocks = block_num;
    return;
  }

  void push_back_block(uint8_t* block) {
    block_start_vec.emplace_back(block);
    return;
  }

  void push_back_firstkey(std::string& firstkey) {
    firstkey_each_block.append(firstkey);
    return;
  }

  void get_uncompressed_size(uint64_t& withpad, uint64_t& withoutpad) {
    withpad = totalsize_with_padding;
    withoutpad = totalsize_without_padding;
  }
  uint8_t get_max_padding_length() { return max_padding_length; }

  void write_string_wo_padding_len(std::string& buf) {
    for (char item : string_wo_padding_length) {
      buf.append(reinterpret_cast<const char*>(&item), sizeof(char));
    }
  }

  void write_block_padding_len(std::string& buf) {
    for (char item : padding_length) {
      buf.append(reinterpret_cast<const char*>(&item), sizeof(char));
    }
  }


 private:
  std::string firstkey_each_block;
  std::vector<uint8_t*> block_start_vec;
  int block_size;
  int blocks;
  uint64_t totalsize_without_padding;
  uint64_t totalsize_with_padding;
  std::vector<uint8_t> padding_length;
  std::vector<uint8_t> string_wo_padding_length;
  std::vector<std::string> padding_max;
  std::vector<std::string> padding_min;
  uint8_t max_padding_length;
};

template <typename T>
inline uint8_t randomdecodeArray8_string(const char* in, int idx, T* result, int length) {
  const uint8_t* tmpin = reinterpret_cast<const uint8_t*>(in);
  uint32_t maxbits;
  memcpy(&maxbits, tmpin, sizeof(uint32_t));
  tmpin += sizeof(uint32_t);
  // std::cout<<"max bit "<<maxbits<<std::endl;
  uint8_t block_pad_length;
  memcpy(&block_pad_length, tmpin, sizeof(uint8_t));
  tmpin += sizeof(uint8_t);

  T theta0;
  memcpy(&theta0, tmpin, sizeof(T));
  tmpin += sizeof(T);

  T theta1;
  memcpy(&theta1, tmpin, sizeof(T));
  tmpin += sizeof(T);

  uint8_t ori_length = 0;
  read_bit_fix_string<T>(tmpin, maxbits, idx, theta1, theta0, result, &ori_length);

  int shift = (block_pad_length - ori_length);
  *result = *result >> (uint8_t)(8 * shift);
  *result = *result<<(uint8_t)(8 * shift);
  int tmp_length = block_pad_length - length;
  if(tmp_length>0) {
    *result = *result >> (uint8_t)(8 * tmp_length);
  }
  else{
    *result = *result << (uint8_t)(8 * (-tmp_length));
  }
  //*result = *result >> (uint8_t)(8 * std::max(block_pad_length - length, 0));

  return ori_length;
}

class Leco_integral {
 public:
  uint8_t* encodeArray8(uint64_t* in, const size_t length, uint8_t* res,
                        size_t nvalue) {
    double* indexes = new double[length];
    double* keys = new double[length];
    // double *keys_sample = new double [length];
    // double *indexes_sample = new double[length];
    uint8_t* out = res;
    for (uint32_t i = 0; i < length; i++) {
      indexes[i] = (double)i;
      keys[i] = (double)in[i];
    }
    long long* delta = new long long[length];

    lr mylr;
    // mylr.caltheta_LOO(indexes,keys,length);
    mylr.caltheta(indexes, keys, length);

    free(indexes);
    free(keys);
    long long max_error = 0;
    for (int i = 0; i < (long long)length; i++) {
      long long tmp =
          (long long)in[i] - (long long)(mylr.theta0 + mylr.theta1 * (double)i);
      delta[i] = tmp;
      if (abs(tmp) > max_error) {
        max_error = abs(tmp);
      }
    }
    //std::cout<<"delta[1] "<<delta[1]<<std::endl;
    int tmp_bit = bits_T<uint64_t>(max_error) + 1;
    out[0] = (uint8_t)tmp_bit;
    out++;
    double theta0 = mylr.theta0;
    double theta1 = mylr.theta1;

    memcpy(out, &theta0, sizeof(theta0));
    out += sizeof(theta0);
    memcpy(out, &theta1, sizeof(theta1));
    out += sizeof(theta1);

    out = write_delta(delta, out, tmp_bit, length);

    free(delta);

    return out;
  }

  inline uint64_t* decodeArray8(uint8_t* in, const size_t length, uint64_t* out,
                                size_t nvalue) {
    // std::cout<<"decompressing all!"<<std::endl;
    double theta0;
    double theta1;
    uint8_t maxerror;
    uint8_t* tmpin = in;
    memcpy(&maxerror, tmpin, 1);
    tmpin++;
    memcpy(&theta0, tmpin, 8);
    tmpin += 8;
    memcpy(&theta1, tmpin, 8);
    tmpin += 8;

    read_all_bit_fix(tmpin, 0, 0, length, maxerror, theta1, theta0, out);

    return out;
  }
};

inline void randomdecodeArray8_integer(const char* in, const size_t l,
                                       uint64_t* out, size_t nvalue) {
  double theta0;
  double theta1;
  uint8_t maxerror;
  const uint8_t* tmpin = reinterpret_cast<const uint8_t*>(in);
  memcpy(&maxerror, tmpin, 1);
  tmpin++;
  memcpy(&theta0, tmpin, 8);
  tmpin += 8;
  memcpy(&theta1, tmpin, 8);
  tmpin += 8;
  uint64_t tmp = 0;

  read_bit_fix_char(tmpin, maxerror, l, theta1, theta0, 0, out);
}

}  // namespace ROCKSDB_NAMESPACE