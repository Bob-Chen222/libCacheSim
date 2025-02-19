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
#include <math.h>
#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
  // fields added in addition to clock-
  int64_t n_insertion;
  int n_promotion;

  //   for online delay
  int64_t miss;
  int64_t vtime;
  uint64_t expected_eviction_age;

  double percentile;

  // profiling
  int64_t sum_hit;
  int64_t sum_diff; //where the predicted information is wrong in making the decision against the actual time
} LRU_delay_params_t;

static const char *DEFAULT_PARAMS = "percentage=0.9";
#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void Delay_online_parse_params(cache_t *cache, const char *cache_specific_params);
static void Delay_online_free(cache_t *cache);
static bool Delay_online_get(cache_t *cache, const request_t *req);
static cache_obj_t *Delay_online_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *Delay_online_insert(cache_t *cache, const request_t *req);
static cache_obj_t *Delay_online_to_evict(cache_t *cache, const request_t *req);
static void Delay_online_evict(cache_t *cache, const request_t *req);
static bool Delay_online_remove(cache_t *cache, const obj_id_t obj_id);
double next_access_time(double prev_arrival, double mean_interarrival, double percentile);

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
cache_t *Delay_online_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("Delay_online", ccache_params, cache_specific_params);
  cache->cache_init = Delay_online_init;
  cache->cache_free = Delay_online_free;
  cache->get = Delay_online_get;
  cache->find = Delay_online_find;
  cache->insert = Delay_online_insert;
  cache->evict = Delay_online_evict;
  cache->remove = Delay_online_remove;
  cache->to_evict = Delay_online_to_evict;
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
  params->percentile = 0.9;
  params->sum_hit = 0;
  params->sum_diff = 0;

  if (cache_specific_params != NULL) {
    Delay_online_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "Delay_online-%f", params->percentile);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void Delay_online_free(cache_t *cache) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  double accuracy = (double)params->sum_diff / (double)params->sum_hit;
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
static bool Delay_online_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

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
static cache_obj_t *Delay_online_find(cache_t *cache, const request_t *req, const bool update_cache) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  params->vtime += 1;
  params->sum_hit += 1;
  cache_obj_t *cache_obj = cache_find_base(cache, req, update_cache);

  //   calculate the expected reuse distance
  double expected_eviction_age, miss_ratio, time_remaining_in_cache;
  miss_ratio = (double)params->miss / (double)params->vtime;
  expected_eviction_age = ((double) params->expected_eviction_age);

  if (cache_obj == NULL) {
    return NULL;
  }
  
  //update the average request arrival time
  cache_obj->delay_count.freq += 1;
  cache_obj->delay_count.sum_dist += (params->vtime - cache_obj->delay_count.last_hit_vtime);
  cache_obj->delay_count.last_hit_vtime = params->vtime;
  time_remaining_in_cache = expected_eviction_age - (params->vtime - cache_obj->delay_count.last_promotion_vtime);

  //calculate next access time
  double arrival_average = (double)cache_obj->delay_count.sum_dist / ((double)cache_obj->delay_count.freq);
  double time_next_access = next_access_time(params->vtime, arrival_average, params->percentile);

  double dist_next_access_predicted = (double)req->next_access_vtime - (double)params->vtime;
  double dist_next_access = (time_next_access - (double)params->vtime);

  bool res1 = (dist_next_access_predicted > time_remaining_in_cache && dist_next_access_predicted < expected_eviction_age);
  bool res2 = (dist_next_access > time_remaining_in_cache && dist_next_access < expected_eviction_age);
  if (res1 != res2) {
    params->sum_diff += 1;
    // printf("arrival_average: %f\n", arrival_average);
    // printf("freq: %d\n", cache_obj->delay_count.freq);
    // printf("dist_next_access: %f\n", dist_next_access);
    // printf("dist_next_access_predicted: %f\n", dist_next_access_predicted);
    // printf("promotion range: (%lf, %lf)\n", time_remaining_in_cache, expected_eviction_age);
    // printf("\n");
  }

  // if (abs(dist_next_access - dist_next_access_predicted) > 1) {
    
  // }

  DEBUG_ASSERT(dist_next_access >= 0);
  /*
  only if the next access is within (time_remaining_in_cache * expected_eviction_age, expected_eviction_age)
  we promote it
  */
  if (cache_obj && likely(update_cache) && (dist_next_access > time_remaining_in_cache && dist_next_access < expected_eviction_age)) {
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
static cache_obj_t *Delay_online_insert(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  params->miss += 1;
  params->n_insertion++;
  cache_obj_t *obj = cache_insert_base(cache, req);
  obj->delay_count.last_promo_vtime = params->n_insertion;
  obj->delay_count.insert_time = params->vtime;// for profiling

  obj->delay_count.last_hit_vtime = params->vtime;
  obj->delay_count.last_promotion_vtime = params->vtime;
  obj->delay_count.freq = 0;
  obj->delay_count.sum_dist = 0;

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
static cache_obj_t *Delay_online_to_evict(cache_t *cache, const request_t *req) {
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
static void Delay_online_evict(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  obj_to_evict->delay_count.last_promo_vtime = 0;
  double eviction_age = (double)params->vtime - obj_to_evict->delay_count.insert_time;
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
static void Delay_online_remove_obj(cache_t *cache, cache_obj_t *obj_to_remove) {
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
static bool Delay_online_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  Delay_online_remove_obj(cache, obj);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *Delay_online_current_params(LRU_delay_params_t *params) {
  static __thread char params_str[128];
  int n = snprintf(params_str, 128, "percentage=%lu\n", params->percentile);
  return params_str;
}

static void Delay_online_parse_params(cache_t *cache, const char *cache_specific_params) {
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

    if (strcasecmp(key, "percentage") == 0) {
      if (strchr(value, '.') != NULL) {
        params->percentile = strtof(value, &end);
      }else{
        if (strlen(end) > 2) {
          ERROR("param parsing error, find string \"%s\" after number\n", end);
        }
      }
    } else {
      ERROR("%s does not have parameter %s\n", cache->cache_name, key);
      exit(1);
    }
  }
  free(old_params_str);
}

double next_access_time(double prev_arrival, double mean_interarrival, double percentile) {
    // Compute lambda from the given mean inter-arrival time
    double lambda = 1.0 / mean_interarrival;
    double p = percentile;
    // Compute the percentile value using the inverse CDF of Exp(lambda)
    double X_p = -log(1 - p) / lambda;

    // Compute and return the estimated next access time
    return prev_arrival + X_p;
}

#ifdef __cplusplus
}
#endif
