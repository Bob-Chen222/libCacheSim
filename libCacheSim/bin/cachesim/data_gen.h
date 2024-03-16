
#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>


static inline uint64_t* generate_zipf(double alpha, uint64_t req_cnt, int64_t obj_num) {
  uint64_t* zipf = (uint64_t*)malloc(req_cnt * sizeof(uint64_t));
  double c = 0; // Normalization constant
  for (int64_t i = 1; i <= obj_num; i++) {
    c = c + (1.0 / pow((double)i, alpha));
  }
  c = 1.0 / c;
  double* dist = (double*)malloc(obj_num * sizeof(double));
  dist[0] = c / pow(1.0, alpha);
  for (int64_t i = 1; i < obj_num; i++) {
    dist[i] = dist[i - 1] + c / pow((double)(i + 1), alpha);
  }
  for (uint64_t i = 0; i < req_cnt; i++) {
    double r = (double)rand() / (double)RAND_MAX;
    int64_t lo = 0;
    int64_t hi = obj_num - 1;
    while (lo < hi) {
      int64_t mid = (lo + hi) / 2;
      if (dist[mid] < r) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    zipf[i] = lo;
  }
  free(dist);
  return zipf;
}
