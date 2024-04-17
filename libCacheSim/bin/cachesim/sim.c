

#include "../../include/libCacheSim/cache.h"
#include "../../include/libCacheSim/reader.h"
#include "../../utils/include/mymath.h"
#include "../../utils/include/mystr.h"
#include "../../utils/include/mysys.h"
#include <pthread.h>
#include <stdatomic.h>
#include "data_gen.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct thread_params {
  uint64_t thread_id;
  cache_t* cache;
  reader_t* reader;
  uint64_t num_threads;
  uint64_t req_cnt;
  uint64_t miss_cnt;
  uint64_t* oracle;
  request_t** req_list;
} thread_params_t;

void* thread_function(void* arg){

  // printf("triggered thread_id: %lu\n", ((thread_params_t*)arg)->thread_id);
  thread_params_t* thread_params = (thread_params_t*)arg;
  uint64_t thread_id = thread_params->thread_id;
  uint64_t num_threads = thread_params->num_threads;
  uint64_t item_size = thread_params->reader->item_size;
  uint64_t req_cnt = thread_params->req_cnt;

  

  // read the file
  uint64_t miss_cnt = 0;
  for (uint64_t i = 0; i < req_cnt / num_threads; i++) {
    request_t* req = thread_params->req_list[i];
    // printf("obj_id: %ld\n", req->obj_id);
    
    // printf("%ld\n", req->obj_id);
    if (!thread_params->cache->get(thread_params->cache, req)) {
      miss_cnt++;
    }
    // move to the next request
  }
  // only used for oracleGeneralBin
  // printf("miss count: %ld\n", miss_cnt);
  atomic_fetch_add(&thread_params->miss_cnt, miss_cnt);
  return NULL;

}


void parallel_simulate(reader_t *reader, cache_t *cache, int report_interval,
             char *ofilepath, int num_threads) {
  /* random seed */
  srand(time(NULL));
  set_rand_seed(rand());

  // printf("num_thread: %lu\n", num_threads);
  int req_cnt = 10000000;
  int obj_num = 100000;
  int warmup_cnt = 100;
  double alpha = 1;

  // printf("generating file\n");
  uint64_t* oracles = generate_zipf(alpha, req_cnt + warmup_cnt, obj_num);
  // printf("generating file finished\n");

  // do warmup for the cache
  request_t* req = new_request();
  for (uint64_t i = 0; i < warmup_cnt; i++) {
    uint32_t real_time = 0;
    uint64_t obj_id = oracles[i];
    uint32_t obj_size = 1;
    int64_t next_access_vtime = -1;
    req->clock_time = real_time;
    req->obj_id = obj_id + 1;
    req->obj_size = obj_size;
    req->next_access_vtime = next_access_vtime;
    if (!cache->find(cache, req, true)) {
      // insert the object into the linked list
      cache->insert(cache, req);
    }
  }
  // printf("warmup finished, cache objects: %lu\n", cache->n_obj);
  cache->warmup_complete = true;
  free_request(req);

  // for (uint64_t i = 0; i < req_cnt; i++) {
  //   printf("oracle: %ld\n", oracles[i]);
  // }


  // preprocessing
  // first version should have lock each round
  // create num_threads threads
  pthread_t threads[num_threads];
  thread_params_t* thread_params = malloc(sizeof(thread_params_t) * num_threads);
  for (uint64_t i = 0; i < num_threads; i++) {
    thread_params[i].thread_id = i;
    thread_params[i].cache = cache;
    thread_params[i].reader = reader;
    thread_params[i].num_threads = num_threads;
    thread_params[i].miss_cnt = 0;
    thread_params[i].req_cnt = req_cnt;
    thread_params[i].oracle = oracles;
    // preload the file
    request_t** req_list = malloc(sizeof(request_t*) * req_cnt / num_threads);
    for (uint64_t j = 0; j < req_cnt / num_threads; j++) {
      req_list[j] = new_request();
      uint32_t real_time = 0;
      uint64_t obj_id = oracles[j * num_threads + i];
      uint32_t obj_size = 1;
      int64_t next_access_vtime = -1;
      req_list[j]->clock_time = real_time;
      req_list[j]->obj_id = obj_id + 1;
      // req_list[j]->obj_id += i * 10000007UL;
      DEBUG_ASSERT(req_list[j]->obj_id != 0);
      req_list[j]->obj_size = obj_size;
      req_list[j]->next_access_vtime = next_access_vtime;
    }
    thread_params[i].req_list = req_list;
  }

  double start_time = gettime();
  for (uint64_t i = 0; i < num_threads; i++) {
    pthread_create(&threads[i], NULL, thread_function, &thread_params[i]);
  }

  for (uint64_t i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }


  double runtime = gettime() - start_time;
  // printf("runtime total: %.8lf\n", runtime);

  char output_str[1024];
  char size_str[8];
  convert_size_to_str(cache->cache_size, size_str);

  uint64_t miss_cnt = 0;
  for (uint64_t i = 0; i < num_threads; i++) {
    miss_cnt += thread_params[i].miss_cnt;
  }

  
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
  snprintf(output_str, 1024,
           "%s %s cache size %8s, %16lu req, miss ratio %.4lf, throughput "
           "%.2lf MQPS, thread_num %d\n",
           reader->trace_path, cache->cache_name, size_str,
           (unsigned long)req_cnt,   (double)miss_cnt / (double)req_cnt,
           (double)req_cnt / 1000000.0 / runtime, num_threads);

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
