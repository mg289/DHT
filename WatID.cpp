#include "WatID.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <algorithm>
#include <openssl/md5.h>

namespace WatDHT {

WatID::WatID(const unsigned char* other_id_array) {
  memcpy(id_array, other_id_array, MD5_DIGEST_LENGTH);
}


int WatID::copy_from(const std::string& other_id) {
  if (other_id.length() != MD5_DIGEST_LENGTH) {
    return -1;  
  }
  memcpy(id_array, other_id.data(), MD5_DIGEST_LENGTH);
  return 0;
}


void WatID::set_using_md5(const std::string& id) {
  MD5(reinterpret_cast<const unsigned char*>(id.data()), id.length(), id_array);
}

const WatID WatID::distance_ccr(const WatID& endpoint) const {
  unsigned char diff_array[MD5_DIGEST_LENGTH];
  int carry_over = 0;
  for (int i = MD5_DIGEST_LENGTH - 1; i >= 0; i--) {
    diff_array[i] = id_array[i] - endpoint.id_array[i] - carry_over;
    if (id_array[i] == 0) {
      carry_over = (endpoint.id_array[i] > 0 || carry_over > 0) ? 1 : 0;
    } else {
      carry_over = ((id_array[i] - carry_over) < endpoint.id_array[i]) ? 1 : 0;
    }
  }
  return WatID(diff_array);
}

int WatID::get_kth_bin(int bin_num, int bin_bit_size) const {
  int bit_index = bin_bit_size * bin_num;
  int byte_index = bit_index / uchar_bits;
  int shift_offset = uchar_bits - (bit_index - byte_index * 
                                   uchar_bits + bin_bit_size);
  return (id_array[byte_index] >> shift_offset) & 
         ((1 << bin_bit_size) - 1);
}

const WatID WatID::distance_cr(const WatID& endpoint) const {
  return endpoint.distance_ccr(*this);
}

const WatID WatID::distance(const WatID& endpoint) const {
  WatID cr = distance_cr(endpoint);
  if (cr.id_array[0] < (1 << (uchar_bits - 1))) {
    return cr; 
  }
  return distance_ccr(endpoint);
}
 
int WatID::hmatch_bin(const WatID& other, int bin_bit_size) const {
  int num_bins = (MD5_DIGEST_LENGTH * uchar_bits) / bin_bit_size;
  for (int i = 0; i < num_bins; ++i) {
    if (get_kth_bin(i, bin_bit_size) != 
        other.get_kth_bin(i, bin_bit_size)) {
      return i - 1; // Return the last matching bin index. 
    }
  }
  return num_bins - 1;  // Matches every bin, return index to the last one. 
}

bool WatID::operator==(const WatID& other) const {
  for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    if (id_array[i] != other.id_array[i]) {
      return false;
    }
  }
  return true;
}

bool WatID::operator<(const WatID& other) const {
  for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    if (id_array[i] < other.id_array[i]) {
      return true;
    } else if (id_array[i] > other.id_array[i]) {
      return false;
    }
  }
  return false;
}

bool WatID::operator>(const WatID& other) const {
  for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    if (id_array[i] < other.id_array[i]) {
      return false;
    } else if (id_array[i] > other.id_array[i]) {
      return true;
    }
  }
  return false;
}

void WatID::debug_md5() const {
  for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    printf("%02x", id_array[i]);
  }
  printf("\n");
}
}
