//
//  a version of delay that use reuse distance, expected reuse distance to
// and the next access time to determine whether to promote or not
//
//
//  LRU.c
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
  // fields added in addition to clock-
  uint64_t delay_time;  // determines how often promotion is performed
  int64_t n_insertion;
  int n_promotion;

  //   for offline delay
  int64_t miss;
  int64_t vtime;
  uint64_t expected_eviction_age;
} LRU_delay_params_t;

static const char *DEFAULT_PARAMS = "delay-time=1";
#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void Delay_offline_parse_params(cache_t *cache, const char *cache_specific_params);
static void Delay_offline_free(cache_t *cache);
static bool Delay_offline_get(cache_t *cache, const request_t *req);
static cache_obj_t *Delay_offline_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *Delay_offline_insert(cache_t *cache, const request_t *req);
static cache_obj_t *Delay_offline_to_evict(cache_t *cache, const request_t *req);
static void Delay_offline_evict(cache_t *cache, const request_t *req);
static bool Delay_offline_remove(cache_t *cache, const obj_id_t obj_id);

static double percentile = 0.5; //parameter

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************
/**
 * @brief initialize a LRU cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params LRU specific parameters, should be NULL
 */
cache_t *Delay_offline_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("Delay_offline", ccache_params, cache_specific_params);
  cache->cache_init = Delay_offline_init;
  cache->cache_free = Delay_offline_free;
  cache->get = Delay_offline_get;
  cache->find = Delay_offline_find;
  cache->insert = Delay_offline_insert;
  cache->evict = Delay_offline_evict;
  cache->remove = Delay_offline_remove;
  cache->to_evict = Delay_offline_to_evict;
  cache->get_occupied_byte = cache_get_occupied_byte_default;

  if (ccache_params.consider_obj_metadata) {
    cache->obj_md_size = 8 * 2;
  } else {
    cache->obj_md_size = 0;
  }

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "LRU_Belady");
#endif

  LRU_delay_params_t *params = malloc(sizeof(LRU_delay_params_t));
  params->q_head = NULL;
  params->q_tail = NULL;
  cache->eviction_params = params;
  params->n_insertion = 0;
  params->n_promotion = 0;
  params->miss = 0;
  params->vtime = 0;
  params->expected_eviction_age = 0;

  if (cache_specific_params != NULL) {
    Delay_offline_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "Delay_offline");

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void Delay_offline_free(cache_t *cache) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
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
static bool Delay_offline_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief check whether an object is in the cache
 *
 * @param cache
 * @param req
 * @param update_cache whether to update the cache,
 *  if true, the object is promoted
 *  and if the object is expired, it is removed from the cache
 * @return true on hit, false on miss
 */
static cache_obj_t *Delay_offline_find(cache_t *cache, const request_t *req, const bool update_cache) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  params->vtime += 1;
  cache_obj_t *cache_obj = cache_find_base(cache, req, update_cache);

  //   calculate the expected reuse distance
  double expected_eviction_age, miss_ratio, time_remaining_in_cache;
  miss_ratio = (double)params->miss / (double)params->vtime;
  expected_eviction_age = ((double) params->expected_eviction_age);
  int64_t dist_next_access = req->next_access_vtime - params->vtime;
  DEBUG_ASSERT(dist_next_access >= 0);

  if (cache_obj == NULL) {
    return NULL;
  }
  
  //update the average request arrival time
  cache_obj->delay_count.freq += 1;
  time_remaining_in_cache = expected_eviction_age - (params->vtime - cache_obj->delay_count.last_promotion_vtime);


  // printf("dist_next_access: %f\n", (double)dist_next_access);
  // printf("expected_eviction_age: %f\n", expected_eviction_age);
  // printf("\n");

  /*
  only if the next access is within (time_remaining_in_cache * expected_eviction_age, expected_eviction_age)
  we promote it
  */
  if (cache_obj && likely(update_cache) && dist_next_access > time_remaining_in_cache && dist_next_access < expected_eviction_age) {
    //  check whether the last access time is greater than the delay time
    move_obj_to_head(&params->q_head, &params->q_tail, cache_obj);
    cache->n_promotion++;
    // update the last access time
    cache_obj->delay_count.last_promo_vtime = params->n_insertion;
    cache_obj->delay_count.insert_time = params->n_insertion;
    cache_obj->delay_count.last_promotion_vtime = params->vtime;
    params->n_promotion++;
  }

  return cache_obj;
}

/**
 * @brief insert an object into the cache,
 * update the hash table and cache metadata
 * this function assumes the cache has enough space
 * and eviction is not part of this function
 *
 * @param cache
 * @param req
 * @return the inserted object
 */
static cache_obj_t *Delay_offline_insert(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  params->miss += 1;
  params->n_insertion++;
  cache_obj_t *obj = cache_insert_base(cache, req);
  obj->delay_count.last_promo_vtime = params->n_insertion;
  obj->delay_count.insert_time = params->vtime;// for profiling

  obj->delay_count.last_hit_vtime = params->vtime;
  obj->delay_count.last_promotion_vtime = params->vtime;
  obj->delay_count.freq = 0;

  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
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
static cache_obj_t *Delay_offline_to_evict(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;

  DEBUG_ASSERT(params->q_tail != NULL || cache->occupied_byte == 0);

  cache->to_evict_candidate_gen_vtime = cache->n_req;
  return params->q_tail;
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 */
static void Delay_offline_evict(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  obj_to_evict->delay_count.last_promo_vtime = 0;
  double eviction_age = (double)(params->vtime - obj_to_evict->delay_count.insert_time);
  if (eviction_age > -0.1 && eviction_age < 0.1) {
    printf("eviction_age: %f\n", eviction_age);
  }
  if (obj_to_evict->delay_count.freq == 0) {
    double weighted_avg1 = 0.15 * eviction_age;
    double weighted_avg2 = 0.85 * (double)params->expected_eviction_age;
    double weighted_avg = weighted_avg1 + weighted_avg2;
    params->expected_eviction_age = weighted_avg;
    
  }
  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
  cache_remove_obj_base(cache, obj_to_evict, true);
}

/**
 * @brief remove the given object from the cache
 * note that eviction should not call this function, but rather call
 * `cache_evict_base` because we track extra metadata during eviction
 *
 * and this function is different from eviction
 * because it is used to for user trigger
 * remove, and eviction is used by the cache to make space for new objects
 *
 * it needs to call cache_remove_obj_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param obj
 */
static void Delay_offline_remove_obj(cache_t *cache, cache_obj_t *obj_to_remove) {
  DEBUG_ASSERT(obj_to_remove != NULL);
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;

  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_remove);
  cache_remove_obj_base(cache, obj_to_remove, true);
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
static bool Delay_offline_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  Delay_offline_remove_obj(cache, obj);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *Delay_offline_current_params(LRU_delay_params_t *params) {
  static __thread char params_str[128];
  int n = snprintf(params_str, 128, "delay-time=%lu\n", params->delay_time);
  return params_str;
}

static void Delay_offline_parse_params(cache_t *cache, const char *cache_specific_params) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  char *params_str = strdup(cache_specific_params);
  char *old_params_str = params_str;
  char *end;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
