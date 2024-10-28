//
//  Quick demotion + lazy promotion v2
//
//  FIFO + Clock
//  the ratio of FIFO is decided dynamically
//  based on the marginal hits on FIFO-ghost and main cache
//  we track the hit distribution of FIFO-ghost and main cache
//  if the hit distribution of FIFO-ghost at pos 0 is larger than
//  the hit distribution of main cache at pos -1,
//  we increase FIFO size by 1
//
//
//  HOTCache.c
//  libCacheSim
//
//  Created by Juncheng on 1/24/23
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include "../../dataStructure/pqueue.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    cache_t *main_cache;

    cache_obj_t **buffer; //a buffer that stores only 5 to 10 objects depend on the performance and will be setup in the init function
    uint64_t buffer_size;
    float buffer_ratio;
    uint64_t main_cache_size;

    uint64_t divisor; //used for admit objects into the buffer
    int highest_freq; //highest frequency in the last epoch
    int slots_buffer; //the current index that we are at in the buffer
    int next_refresh_time; //the next time we refresh the buffer
    int threshold_to_buffer;
    uint64_t miss;

    // main cache
    char main_cache_type[32];

    bool init; //whether we are still choosing the selection

    // profiling
    int found_in_buffer;
    int round;
    int request_epoch;
    int num_hit_main_cache;
    int promotion_epoch;
    int promotion_saved_epoch;

} HOTCache_params_t;

static const char *DEFAULT_CACHE_PARAMS =
    "main-cache=Clock,size-buffer=0.1,admission-divisor=100";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************
cache_t *HOTCache_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);
static void HOTCache_free(cache_t *cache);
static bool HOTCache_get(cache_t *cache, const request_t *req);

static cache_obj_t *HOTCache_find(cache_t *cache, const request_t *req,
                                const bool update_cache);
static cache_obj_t *HOTCache_insert(cache_t *cache, const request_t *req);
static cache_obj_t *HOTCache_to_evict(cache_t *cache, const request_t *req);
static void HOTCache_evict(cache_t *cache, const request_t *req);
static bool HOTCache_remove(cache_t *cache, const obj_id_t obj_id);
static inline int64_t HOTCache_get_occupied_byte(const cache_t *cache);
static inline int64_t HOTCache_get_n_obj(const cache_t *cache);
static inline bool HOTCache_can_insert(cache_t *cache, const request_t *req);
static void HOTCache_parse_params(cache_t *cache,
                                const char *cache_specific_params);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

cache_t *HOTCache_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("HOTCache", ccache_params, cache_specific_params);
  cache->cache_init = HOTCache_init;
  cache->cache_free = HOTCache_free;
  cache->get = HOTCache_get;
  cache->find = HOTCache_find;
  cache->insert = HOTCache_insert;
  cache->evict = HOTCache_evict;
  cache->remove = HOTCache_remove;
  cache->to_evict = HOTCache_to_evict;
  cache->get_n_obj = HOTCache_get_n_obj;
  cache->get_occupied_byte = HOTCache_get_occupied_byte;
  cache->can_insert = HOTCache_can_insert;

  cache->obj_md_size = 0;

  cache->eviction_params = malloc(sizeof(HOTCache_params_t));
  memset(cache->eviction_params, 0, sizeof(HOTCache_params_t));
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;

  HOTCache_parse_params(cache, DEFAULT_CACHE_PARAMS);
  if (cache_specific_params != NULL) {
    HOTCache_parse_params(cache, cache_specific_params);
  }
 
  params->buffer = malloc(sizeof(cache_obj_t *) * params->buffer_size);
  // choose 0-9
  for (int i = 0; i < params->buffer_size; i++){
    cache_obj_t *obj = malloc(sizeof(cache_obj_t));
    obj -> obj_id = i;
    params->buffer[i] = obj;
  }
  int64_t main_cache_size = ccache_params.cache_size - params->buffer_size;
  params->main_cache_size = main_cache_size;
  DEBUG_ASSERT(main_cache_size > 0);

  common_cache_params_t ccache_params_local = ccache_params;
  ccache_params_local.cache_size = main_cache_size;
  if (strcasecmp(params->main_cache_type, "FIFO") == 0) {
    params->main_cache = FIFO_init(ccache_params_local, NULL);
  } else if (strcasecmp(params->main_cache_type, "clock") == 0) {
    params->main_cache = Clock_init(ccache_params_local, NULL);
  } else if (strcasecmp(params->main_cache_type, "clock2") == 0) {
    params->main_cache = Clock_init(ccache_params_local, "n-bit-counter=2");
  } else if (strcasecmp(params->main_cache_type, "clock3") == 0) {
    params->main_cache = Clock_init(ccache_params_local, "n-bit-counter=3");
  } else if (strcasecmp(params->main_cache_type, "lru") == 0) {
    params->main_cache = LRU_init(ccache_params_local, NULL);
  } else if (strcasecmp(params->main_cache_type, "lruprob1") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.1");
  } else if (strcasecmp(params->main_cache_type, "lruprob2") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.2");
  } else if (strcasecmp(params->main_cache_type, "lruprob3") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.3");
  } else if (strcasecmp(params->main_cache_type, "lruprob4") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.4");
  } else if (strcasecmp(params->main_cache_type, "lruprob5") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.5");
  } else if (strcasecmp(params->main_cache_type, "lruprob6") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.6");
  } else if (strcasecmp(params->main_cache_type, "lruprob7") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.7");
  } else if (strcasecmp(params->main_cache_type, "lruprob8") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.8");
  } else if (strcasecmp(params->main_cache_type, "lruprob9") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.9");
  } else if (strcasecmp(params->main_cache_type, "lrudelay1") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.1");
  } else if (strcasecmp(params->main_cache_type, "lrudelay2") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.2");
  } else if (strcasecmp(params->main_cache_type, "lrudelay3") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.3");
  } else if (strcasecmp(params->main_cache_type, "lrudelay4") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.4");
  } else if (strcasecmp(params->main_cache_type, "lrudelay5") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.5");
  } else if (strcasecmp(params->main_cache_type, "lrudelay6") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.6");
  } else if (strcasecmp(params->main_cache_type, "lrudelay7") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.7");
  } else if (strcasecmp(params->main_cache_type, "lrudelay8") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.8");
  } else if (strcasecmp(params->main_cache_type, "lrudelay9") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.9");
  } else if (strcasecmp(params->main_cache_type, "lrubatch1") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.1");
  } else if (strcasecmp(params->main_cache_type, "lrubatch2") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.2");
  } else if (strcasecmp(params->main_cache_type, "lrubatch3") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.3");
  } else if (strcasecmp(params->main_cache_type, "lrubatch4") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.4");
  } else if (strcasecmp(params->main_cache_type, "lrubatch5") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.5");
  } else if (strcasecmp(params->main_cache_type, "lrubatch6") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.6");
  } else if (strcasecmp(params->main_cache_type, "lrubatch7") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.7");
  } else if (strcasecmp(params->main_cache_type, "lrubatch8") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.8");
  } else if (strcasecmp(params->main_cache_type, "lrubatch9") == 0) {
    params->main_cache = lpFIFO_batch_init(ccache_params_local, "batch-size=0.9");
  } else{
    ERROR("HOTCache: main cache type %s is not supported\n",
          params->main_cache_type);
  }

  // initalize every object id to be -1
  for (int i = 0; i < params->buffer_size; i++){
    params->buffer[i] = NULL;
  }
  // params->pq = pqueue_init((unsigned long)8e6);
  params->init = true;
  params->found_in_buffer = 0;
  params->highest_freq = 0;
  params->slots_buffer = 0;
  params->next_refresh_time = -1;
  params->miss = 0;
  params->request_epoch = 0;
  params->promotion_epoch = 0;
  params->threshold_to_buffer = 0;
  params->promotion_saved_epoch = 0;
  params->num_hit_main_cache = 0;

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "HOTCache-%s-%.2f-%d",
           params->main_cache_type, params->buffer_ratio, params->divisor);
  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void HOTCache_free(cache_t *cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  params->main_cache->cache_free(params->main_cache);
  printf("num hit main cache: %d\n", params -> num_hit_main_cache);
  free(params->buffer);
  free(cache->eviction_params);
  cache_struct_free(cache);
}

/**
 * @brief this function is the user facing API
 * it performs the following logic
 *
 * ```
 * if obj in cache:
 *    update_metadata
 *    return true
 * else:
 *    if cache does not have enough space:
 *        evict until it has space to insert
 *    insert the object
 *    return false
 * ```
 *
 * @param cache
 * @param req
 * @return true if cache hit, false if cache miss
 */
static bool HOTCache_get(cache_t *cache, const request_t *req) {
  // first try to find the object in the cache
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  cache_obj_t *obj = HOTCache_find(cache, req, true);
  bool hit = (obj != NULL);
  if (hit) {
  } else if (!cache->can_insert(cache, req)) {
    printf("cannot insert obj %ld\n", req -> obj_id);
  } else {
    cache->n_insert += 1;
    while (cache->get_occupied_byte(cache) + req->obj_size +
               cache->obj_md_size >
           params -> main_cache_size) {
      HOTCache_evict(cache, req);
    }
    HOTCache_insert(cache, req);
  }
  // cache -> n_promotion = params -> main_cache -> n_promotion;
  return hit;
}

// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************
/**
 * @brief find an object in the cache
 *
 * @param cache
 * @param req
 * @param update_cache whether to update the cache,
 *  if true, the object is promoted
 *  and if the object is expired, it is removed from the cache
 * @return the object or NULL if not found
 */
static cache_obj_t *HOTCache_find(cache_t *cache, const request_t *req,
                                const bool update_cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  cache_obj_t *cached_obj = NULL;
  params -> request_epoch++;

  if ((params -> slots_buffer == params->buffer_size && params -> next_refresh_time == -1) || params -> next_refresh_time == cache -> n_req){
    //  expected reuse distance is cache size / miss ratio
    float miss_ratio = (float)params -> miss / (float)cache -> n_req;
    int expected_reuse_distance = (int)((float)cache -> cache_size / miss_ratio);
    if (params -> next_refresh_time == -1){
      params -> next_refresh_time = cache -> n_req + expected_reuse_distance;
    }else{
      params -> next_refresh_time = cache -> n_req + expected_reuse_distance;
    }
    params -> threshold_to_buffer = params -> divisor;
    params -> slots_buffer = 0;
    params -> highest_freq = 0;
    params -> request_epoch = 0;
    printf("refresh buffer\n");
  }

  // if update cache is false, we only check the fifo and main caches
  DEBUG_ASSERT(update_cache == true); // only support this mode
  cache_obj_t *obj_buf = hashtable_find_obj_id(cache -> hashtable, req -> obj_id);
  if (obj_buf != NULL){
    obj_buf -> freq++;
    if (obj_buf -> freq > params -> highest_freq){
      params -> highest_freq = obj_buf -> freq;
    }
    params -> found_in_buffer++;
    return obj_buf;
  }


  // if the cache is in the init phase, we need to run the main cache
  cached_obj = params->main_cache->find(params->main_cache, req, update_cache);
  if (cached_obj == NULL) return NULL;
  params -> num_hit_main_cache++;
  cache -> type1++;
  cached_obj->freq++;
  if (cached_obj->freq > params->highest_freq){
    params->highest_freq = cached_obj->freq;
  }
  // if it meets the threshold, we will put it into the buffer
  if (cached_obj -> freq >= params -> threshold_to_buffer && params -> slots_buffer < params -> buffer_size){
    // delete the previous object in the hashtable
    // get the candidate object
    cache_obj_t *candidate_to_delete = params -> buffer[params -> slots_buffer];
    if (candidate_to_delete && hashtable_find_obj_id(cache -> hashtable, candidate_to_delete -> obj_id)){
      hashtable_delete_obj_id(cache -> hashtable, candidate_to_delete -> obj_id);
    }

    // do a strong check
    if (cache -> hashtable -> n_obj > params -> buffer_size){
      ERROR("HOTCache: buffer size is not enough\n");
      exit(1);
    }
    if (params -> main_cache -> cache_size > cache -> cache_size - params -> buffer_size){
      ERROR("HOTCache: main cache size is not enough\n");
      printf("main cache size: %ld\n", params -> main_cache -> n_obj);
      printf("cache size: %ld\n", cache -> cache_size);
      exit(1);
    }
    if (params -> main_cache -> hashtable -> n_obj > cache -> cache_size - params -> buffer_size){
      // printf("main cache hashtable size: %ld\n", params -> main_cache -> hashtable -> n_obj);
      // printf("cache size: %ld\n", cache -> cache_size);
      // printf("buffer size: %ld\n", params -> buffer_size);
      ERROR("HOTCache: main cache hashtable size is not enough\n");
    }
    
    cache_obj_t* new = hashtable_insert(cache -> hashtable, req);
    params -> buffer[params -> slots_buffer] = new;
    new -> freq = cached_obj -> freq;
    printf("insert obj %ld into buffer with freq %d\n", new -> obj_id, new -> freq);
    params -> slots_buffer++;
  }
  return cached_obj;
}

/**
 * @brief insert an object into the cache,
 * update the hash table and cache metadata
 * this function assumes the cache has enough space
 * eviction should be
 * performed before calling this function
 *
 * @param cache
 * @param req
 * @return the inserted object
 */
static cache_obj_t *HOTCache_insert(cache_t *cache, const request_t *req) {
  // do the regular insertion
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  params -> miss++;
  DEBUG_ASSERT(!hashtable_find_obj_id(params->main_cache->hashtable, req->obj_id));
  cache_obj_t *obj = params->main_cache->insert(params->main_cache, req);
  // // also need to insert the object into the base
  // // cache_insert_base(cache, req);
  obj -> last_access_time = cache -> n_insert;
  cache->occupied_byte +=
      (int64_t)obj->obj_size + (int64_t)cache->obj_md_size;
  cache->n_obj += 1;
  // every 1 million requests, we just update the buffer
  obj -> freq = 0;
  return obj;
}

/**
 * @brief find the object to be evicted
 * this function does not actually evict the object or update metadata
 * not all eviction algorithms support this function
 * because the eviction logic cannot be decoupled from finding eviction
 * candidate, so use assert(false) if you cannot support this function
 *
 * @param cache the cache
 * @return the object to be evicted
 */
static cache_obj_t *HOTCache_to_evict(cache_t *cache, const request_t *req) {
  assert(false);
  return NULL;
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 * @param evicted_obj if not NULL, return the evicted object to caller
 */
static void HOTCache_evict(cache_t *cache, const request_t *req) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  params->main_cache->evict(params->main_cache, req);
  cache->occupied_byte -= 1; //assert occupied byte is 1
  cache->n_obj -= 1;
  // everytime we update the n_promotion
  cache -> n_promotion = params -> main_cache -> n_promotion;
  return;
}

/**
 * @brief remove an object from the cache
 * this is different from cache_evict because it is used to for user trigger
 * remove, and eviction is used by the cache to make space for new objects
 *
 * it needs to call cache_remove_obj_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param obj_id
 * @return true if the object is removed, false if the object is not in the
 * cache
 */
static bool HOTCache_remove(cache_t *cache, const obj_id_t obj_id) {
  assert(false);
  return false;
  // HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  // bool removed = false;
  // removed = removed || params->fifo->remove(params->fifo, obj_id);
  // removed = removed || params->fifo_ghost->remove(params->fifo_ghost, obj_id);
  // removed = removed || params->main_cache->remove(params->main_cache, obj_id);

  // return removed;
}

static inline int64_t HOTCache_get_occupied_byte(const cache_t *cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  return params->main_cache->get_occupied_byte(params->main_cache);
}

static inline int64_t HOTCache_get_n_obj(const cache_t *cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  return params->main_cache->get_n_obj(params->main_cache);
}

static inline bool HOTCache_can_insert(cache_t *cache, const request_t *req) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;

  return req->obj_size <= params->main_cache->cache_size;
}

// ***********************************************************************
// ****                                                               ****
// ****                parameter set up functions                     ****
// ****                                                               ****
// ***********************************************************************
static const char *HOTCache_current_params(HOTCache_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "main-cache=%s\n",
           params->main_cache->cache_name);
  return params_str;
}

static void HOTCache_parse_params(cache_t *cache,
                                const char *cache_specific_params) {
  HOTCache_params_t *params = (HOTCache_params_t *)(cache->eviction_params);

  char *params_str = strdup(cache_specific_params);
  char *old_params_str = params_str;
  // char *end;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }

    if (strcasecmp(key, "main-cache") == 0) {
      strncpy(params->main_cache_type, value, 30);
    } else if (strcasecmp(key, "size-buffer") == 0) {
      params->buffer_ratio = atof(value);
      params->buffer_size = (uint64_t)(atof(value) * cache->cache_size);
    } else if (strcasecmp(key, "admission-divisor") == 0) {
      params->divisor = (uint64_t)atol(value);
    } else if (strcasecmp(key, "print") == 0) {
      printf("parameters: %s\n", HOTCache_current_params(params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s\n", cache->cache_name, key);
      exit(1);
    }
  }

  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
