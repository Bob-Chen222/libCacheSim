//
// PredClock that estimates the next reuse distance and evict the objects
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include <float.h>

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY
#undef USE_BELADY

static const char *DEFAULT_PARAMS = "scaler=1.0,mode=3,threshold=0.1,interval=1";
static int false_negative = 0;
static int total_negative = 0;

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void PredClock_parse_params(cache_t *cache, const char *cache_specific_params);
static void PredClock_free(cache_t *cache);
static bool PredClock_get(cache_t *cache, const request_t *req);
static cache_obj_t *PredClock_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *PredClock_insert(cache_t *cache, const request_t *req);
static cache_obj_t *PredClock_to_evict(cache_t *cache, const request_t *req);
static void PredClock_evict(cache_t *cache, const request_t *req);
static bool PredClock_remove(cache_t *cache, const obj_id_t obj_id);
static bool is_retained(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance);
static bool is_retained1(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance);
static bool is_retained2(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance);
static bool is_retained3(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief initialize a PredClock cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params PredClock specific parameters as a string
 */
cache_t *PredClock_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("PredClock", ccache_params, cache_specific_params);
  cache->cache_init = PredClock_init;
  cache->cache_free = PredClock_free;
  cache->get = PredClock_get;
  cache->find = PredClock_find;
  cache->insert = PredClock_insert;
  cache->evict = PredClock_evict;
  cache->remove = PredClock_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = PredClock_to_evict;
  cache->obj_md_size = 0;

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "PredClock_Belady");
#endif

  cache->eviction_params = malloc(sizeof(PredClock_params_t));
  memset(cache->eviction_params, 0, sizeof(PredClock_params_t));
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->n_bit_counter = 1;
  params->max_freq = 1;
  params->scaler = 1.0;
  params->num_reinsert = 0;
  params->counter_insert = 0;
  params->interval = 1;

  PredClock_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    PredClock_parse_params(cache, cache_specific_params);
  }

  printf("scaler is : %f\n", params->scaler);

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "PredClock-%d-%f-%d", params->mode, params->threshold, params->interval);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void PredClock_free(cache_t *cache) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  double fn_rate = (double) false_negative / (double) total_negative;
  printf("false negative rate: %f\n", fn_rate);
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
static bool PredClock_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

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
static cache_obj_t *PredClock_find(cache_t *cache, const request_t *req, const bool update_cache) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  params->vtime += 1;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (obj != NULL && update_cache) {
    if (obj->predClock.freq < params->max_freq * 1) {
      // added a threshold!!
      double threshold = params->threshold;
      if (params->counter_insert - obj->predClock.pos >= threshold * cache -> cache_size) {
        obj->predClock.freq += 1; 
      }else{
        // printf("here\n");
      }
    }
    obj->predClock.hit_freq += 1;
    obj->predClock.total_freq += 1;
    // obj->predClock.scale *= params->scaler;
    obj->predClock.reuse_dst = params->vtime - obj->predClock.last_access_vtime;
    obj->predClock.last_access_vtime = params->vtime;
#ifdef USE_BELADY
    obj->next_access_vtime = req->next_access_vtime;
#endif
  }

  return obj;
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
static cache_obj_t *PredClock_insert(cache_t *cache, const request_t *req) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  params->miss += 1;
  params->counter_insert += 1;

  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

  obj->predClock.last_access_vtime = params->vtime;
  obj->predClock.reuse_dst = INT64_MAX;
  obj->predClock.freq = 0;
  obj->predClock.hit_freq = 0;
  obj->predClock.total_freq = 0;
  obj->predClock.scale = 1.0;
  obj->predClock.loop_travel_time = 0;
  obj->predClock.pos = params->counter_insert;
#ifdef USE_BELADY
  obj->next_access_vtime = req->next_access_vtime;
#endif

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
static cache_obj_t *PredClock_to_evict(cache_t *cache, const request_t *req) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;

  int n_round = 0;
  cache_obj_t *obj_to_evict = params->q_tail;
#ifdef USE_BELADY
  while (obj_to_evict->next_access_vtime != INT64_MAX) {
#else
  while (obj_to_evict->predClock.freq - n_round >= 1) {
#endif
    obj_to_evict = obj_to_evict->queue.prev;
    if (obj_to_evict == NULL) {
      obj_to_evict = params->q_tail;
      n_round += 1;
    }
  }

  return obj_to_evict;
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
static void PredClock_evict(cache_t *cache, const request_t *req) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  double miss_ratio;
  miss_ratio = (double)params->miss / (double)params->vtime;
  double expected_reuse_distance = (double)cache->cache_size / miss_ratio;

  cache_obj_t *obj_to_evict = params->q_tail;
  assert(params->vtime >= obj_to_evict->predClock.last_access_vtime);  // Prevent underflow
  assert((double)(params->vtime - obj_to_evict->predClock.last_access_vtime) <=
         DBL_MAX / params->scaler);  // Prevent overflow in multiplication

  double estimated_reuse_distance = (params->vtime - obj_to_evict->predClock.last_access_vtime) * params->scaler;

  bool retained = is_retained(cache, obj_to_evict, expected_reuse_distance);
  while (obj_to_evict->predClock.freq > 0 && retained && obj_to_evict->predClock.check_time != params->vtime) {
    params->counter_insert += 1;
    obj_to_evict->predClock.loop_travel_time += 1;
    obj_to_evict->predClock.freq -= 1;
    params->n_obj_rewritten += 1;
    params->n_byte_rewritten += obj_to_evict->obj_size;
    move_obj_to_head(&params->q_head, &params->q_tail, obj_to_evict);
    cache->n_promotion += 1;
    obj_to_evict->predClock.check_time = params->vtime;
    obj_to_evict->predClock.pos = params->counter_insert;
    obj_to_evict = params->q_tail;
    retained = is_retained(cache, obj_to_evict, expected_reuse_distance);
  }

  params->num_reinsert = 0;

  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
  cache_evict_base(cache, obj_to_evict, true);
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
static void PredClock_remove_obj(cache_t *cache, cache_obj_t *obj) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;

  DEBUG_ASSERT(obj != NULL);
  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
  cache_remove_obj_base(cache, obj, true);
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
static bool PredClock_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  PredClock_remove_obj(cache, obj);

  return true;
}

static bool is_retained(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  bool retained;
  switch (params->mode) {
    case 1:
      retained = is_retained1(cache, obj_to_evict, expected_reuse_distance);
      break;
    case 2:
      retained = is_retained2(cache, obj_to_evict, expected_reuse_distance);
      break;
    case 3:
      retained = is_retained3(cache, obj_to_evict, expected_reuse_distance);
      break;
    default:
      printf("mode not supported\n");
      exit(1);
  }
  return retained;
}

static bool is_retained1(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance) {
  if (obj_to_evict->predClock.freq == 0) {
    return false;
  }
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  double reuse_age = (params->vtime - obj_to_evict->predClock.last_access_vtime);
  double previous_reuse_distance = obj_to_evict->predClock.reuse_dst;

  // stats purpose
  int64_t actual_next_reuse_time = obj_to_evict->misc.next_access_vtime - params->vtime;

  int next_reuse_time_prediction;
  // next_reuse_time_prediction = MAX(previous_reuse_distance * params->scaler, (int)reuse_age * params->scaler);
  next_reuse_time_prediction = (int)reuse_age * params->scaler;

  if (obj_to_evict->predClock.freq != 0 && obj_to_evict->predClock.loop_travel_time > 0) {
    // printf("reuse_age: %f, expected_reuse_distance: %f, previous_reuse_distance: %f, actual_next_reuse_time: %ld, total_freq: %d,hit_freq: %d, loop_travel_time: %d\n",
    //       reuse_age, expected_reuse_distance, previous_reuse_distance, actual_next_reuse_time, obj_to_evict->predClock.total_freq, obj_to_evict->predClock.hit_freq, obj_to_evict->predClock.loop_travel_time);
  }

  if (next_reuse_time_prediction < expected_reuse_distance && obj_to_evict->predClock.freq != 0) {
    obj_to_evict->predClock.hit_freq = 0;
    return true;
  } else {
    if (next_reuse_time_prediction >= expected_reuse_distance && actual_next_reuse_time < expected_reuse_distance){
      false_negative += 1;
      // return true;
      // printf("reuse_age: %f, expected_reuse_distance: %f, previous_reuse_distance: %f, actual_next_reuse_time: %ld, total_freq: %d,hit_freq: %d, loop_travel_time: %d\n",
      //       reuse_age, expected_reuse_distance, previous_reuse_distance, actual_next_reuse_time, obj_to_evict->predClock.total_freq, obj_to_evict->predClock.hit_freq, obj_to_evict->predClock.loop_travel_time);
    }

    total_negative += 1;

    return false;
  }

  return false;
}

static bool is_retained2(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance) {

  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;

  // x is the parameter
  int x = params->interval;
  if (params->num_reinsert % x == x - 1) {
    params->num_reinsert += 1;
    return false;
  }
  params->num_reinsert += 1;

  return true;
}

static bool is_retained3(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance) {
  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *PredClock_current_params(cache_t *cache, PredClock_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "scaler=%d\n", params->scaler);

  return params_str;
}

static void PredClock_parse_params(cache_t *cache, const char *cache_specific_params) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "scaler") == 0) {
      params->n_bit_counter = 1;
      params->scaler = (float)strtod(value, &end);
      params->max_freq = 1;
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", PredClock_current_params(cache, params));
      exit(0);
    } else if (strcasecmp(key, "mode") == 0) {
      params->mode = (int)strtol(value, &end, 10);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "threshold") == 0) {
      params->threshold = (float)strtod(value, &end);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "interval") == 0) {
      params->interval = (int)strtol(value, &end, 10);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    }
    else {
      ERROR("%s does not have parameter %s, example parameters %s\n", cache->cache_name, key,
            PredClock_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
