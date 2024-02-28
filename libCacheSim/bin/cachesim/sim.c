

#include "../../include/libCacheSim/cache.h"
#include "../../include/libCacheSim/reader.h"
#include "../../utils/include/mymath.h"
#include "../../utils/include/mystr.h"
#include "../../utils/include/mysys.h"
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct parallel_simulator_params {
  uint64_t req_cnt;
  uint64_t miss_cnt;
  uint64_t req_byte;
  uint64_t miss_byte;
  uint64_t last_req_cnt;
  uint64_t last_miss_cnt;
  uint64_t num_threads;
  reader_t* reader;
  cache_t* cache;
} parallel_simulator_params_t;

typedef struct thread_params {
  uint64_t mmap_offset; // current position in the file
  uint64_t thread_id;
  parallel_simulator_params_t* params;
} thread_params_t;

void* thread_function(void* arg){

  printf("triggered thread_id: %lu\n", ((thread_params_t*)arg)->thread_id);
  printf("item_size: %lu\n", ((thread_params_t*)arg)->params->reader->item_size);
  thread_params_t* thread_params = (thread_params_t*)arg;
  parallel_simulator_params_t* params = thread_params->params;
  uint64_t thread_id = thread_params->thread_id;
  uint64_t mmap_offset = thread_params->mmap_offset;
  uint64_t num_threads = params->num_threads;
  uint64_t item_size = params->reader->item_size;

  // read the file
  char* start = (char*)(params->reader->mapped_file + mmap_offset);
  request_t* req = new_request();
  double before_time = gettime();
  while (mmap_offset < params->reader->file_size) {
    // printf("time: %f\n", gettime()- before_time);
    uint32_t real_time = *(uint32_t *)start;
    uint64_t obj_id = *(uint64_t *)(start + 4);
    uint32_t obj_size = *(uint32_t *)(start + 12);
    int64_t next_access_vtime = *(int64_t *)(start + 16);
    // because we will ignore the object size, we will just treat it as 1
    obj_size = 1;

    if (next_access_vtime == -1) {
      next_access_vtime = INT64_MAX;
    }

    // obj_id = 1;

    // create a request
    req->clock_time = real_time;
    req->obj_id = obj_id;
    req->obj_size = obj_size;
    req->next_access_vtime = next_access_vtime;

    // simulate the request
    DEBUG_ASSERT(params!= NULL);
    DEBUG_ASSERT(params->cache != NULL);
    if (params->cache->get(params->cache, req) == false) {
      params->miss_cnt++;
      params->miss_byte += req->obj_size;
    }
    params->req_cnt++;
    params->req_byte += req->obj_size;

    // move to the next request
    mmap_offset += item_size * num_threads;
    start = (char*)(params->reader->mapped_file + mmap_offset);
    // add a barrier
    // pthread_barrier_wait(&params->barrier);
  }
  printf("exit loop!!!!!\n");
  double after_time = gettime();
  double runtime = after_time - before_time;
  // only used for oracleGeneralBin
  printf("thread %lu finished\n", thread_id);
  printf("req_cnt: %lu, miss_cnt: %lu\n", params->req_cnt, params->miss_cnt);
  printf("runtime: %.8lf\n", runtime);
  return NULL;

}

void parallel_simulate(reader_t *reader, cache_t *cache, int report_interval,
              int warmup_sec, char *ofilepath, uint64_t num_threads) {
  /* random seed */
  srand(time(NULL));
  set_rand_seed(rand());

  printf("triggered parallel_simulate\n");

  // preprocessing
  // first version should have lock each round
  // create num_threads threads
  printf("num_thread: %lu\n", num_threads);
  pthread_t threads[num_threads];
  thread_params_t* thread_params = (thread_params_t*)malloc(sizeof(thread_params_t) * num_threads);
  for (uint64_t i = 0; i < num_threads; i++) {
    parallel_simulator_params_t* params = (parallel_simulator_params_t*)malloc(sizeof(parallel_simulator_params_t));
    params->req_cnt = 0;
    params->miss_cnt = 0;
    params->req_byte = 0;
    params->miss_byte = 0;
    params->last_req_cnt = 0;
    params->last_miss_cnt = 0;
    params->num_threads = num_threads;
    // params->barrier = barrier;
    params->reader = reader;
    params->cache = cache;
    thread_params[i].thread_id = i;
    thread_params[i].params = params;
    thread_params[i].mmap_offset = i * reader->item_size;
  }

  double start_time = gettime();
  for (uint64_t i = 0; i < num_threads; i++) {
    pthread_create(&threads[i], NULL, thread_function, &thread_params[i]);
  }

  for (uint64_t i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }

  double runtime = gettime() - start_time;
  printf("runtime: %.2lf\n", runtime);

  char output_str[1024];
  char size_str[8];
  convert_size_to_str(cache->cache_size, size_str);

  uint64_t req_cnt = 0, miss_cnt = 0;
  for (uint64_t i = 0; i < num_threads; i++) {
    req_cnt += thread_params[i].params->req_cnt;
    miss_cnt += thread_params[i].params->miss_cnt;
  }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
  snprintf(output_str, 1024,
           "%s %s cache size %8s, %16lu req, miss ratio %.4lf, throughput "
           "%.2lf MQPS\n",
           reader->trace_path, cache->cache_name, size_str,
           (unsigned long)req_cnt,   (double)miss_cnt / (double)req_cnt,
           (double)req_cnt / 1000000.0 / runtime);

#pragma GCC diagnostic pop
  printf("%s", output_str);

  FILE *output_file = fopen(ofilepath, "a");
  if (output_file == NULL) {
    ERROR("cannot open file %s %s\n", ofilepath, strerror(errno));
    exit(1);
  }
  fprintf(output_file, "%s\n", output_str);
  fclose(output_file);

  // do the free
  for (uint64_t i = 0; i < num_threads; i++) {
    free(thread_params[i].params);
  }
  free(thread_params);

#if defined(TRACK_EVICTION_V_AGE)
  while (cache->get_occupied_byte(cache) > 0) {
    cache->evict(cache, req);
  }

#endif
}

#ifdef __cplusplus
}
#endif

void simulate(reader_t *reader, cache_t *cache, int report_interval,
              int warmup_sec, char *ofilepath) {
  /* random seed */
  srand(time(NULL));
  set_rand_seed(rand());

  request_t *req = new_request();
  uint64_t req_cnt = 0, miss_cnt = 0;
  uint64_t last_req_cnt = 0, last_miss_cnt = 0;
  uint64_t req_byte = 0, miss_byte = 0;

  read_one_req(reader, req);
  uint64_t start_ts = (uint64_t)req->clock_time;
  uint64_t last_report_ts = warmup_sec;

  double start_time = -1;
  while (req->valid) {
    req->clock_time -= start_ts;
    if (req->clock_time <= warmup_sec) {
      cache->get(cache, req);
      read_one_req(reader, req);
      continue;
    } else {
      if (start_time < 0) {
        start_time = gettime();
      }
    }

    req_cnt++;
    req_byte += req->obj_size;
    // printf("lalala\n");
    if (cache->get(cache, req) == false) {
      miss_cnt++;
      miss_byte += req->obj_size;
    }
    // printf("fdafa\n");
    if (req->clock_time - last_report_ts >= report_interval &&
        req->clock_time != 0) {
      INFO(
          "%s %s %.2lf hour: %lu requests, miss ratio %.4lf, interval miss "
          "ratio "
          "%.4lf\n",
          mybasename(reader->trace_path), cache->cache_name,
          (double)req->clock_time / 3600, (unsigned long)req_cnt,
          (double)miss_cnt / req_cnt,
          (double)(miss_cnt - last_miss_cnt) / (req_cnt - last_req_cnt));
      last_miss_cnt = miss_cnt;
      last_req_cnt = req_cnt;
      last_report_ts = (int64_t)req->clock_time;
    }

    read_one_req(reader, req);
  }

  double runtime = gettime() - start_time;

  char output_str[1024];
  char size_str[8];
  convert_size_to_str(cache->cache_size, size_str);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
  snprintf(output_str, 1024,
           "%s %s cache size %8s, %16lu req, miss ratio %.4lf, throughput "
           "%.2lf MQPS\n",
           reader->trace_path, cache->cache_name, size_str,
           (unsigned long)req_cnt, (double)miss_cnt / (double)req_cnt,
           (double)req_cnt / 1000000.0 / runtime);

#pragma GCC diagnostic pop
  printf("%s", output_str);

  FILE *output_file = fopen(ofilepath, "a");
  if (output_file == NULL) {
    ERROR("cannot open file %s %s\n", ofilepath, strerror(errno));
    exit(1);
  }
  fprintf(output_file, "%s\n", output_str);
  fclose(output_file);

#if defined(TRACK_EVICTION_V_AGE)
  while (cache->get_occupied_byte(cache) > 0) {
    cache->evict(cache, req);
  }

#endif
}

#ifdef __cplusplus
}
#endif
