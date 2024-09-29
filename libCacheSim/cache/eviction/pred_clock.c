//
// PredClock that estimates the next reuse distance and evict the objects
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include <float.h>

#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY
#undef USE_BELADY

static const char *DEFAULT_PARAMS = "scaler=1.5";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void PredClock_parse_params(cache_t *cache,
                               const char *cache_specific_params);
static void PredClock_free(cache_t *cache);
static bool PredClock_get(cache_t *cache, const request_t *req);
static cache_obj_t *PredClock_find(cache_t *cache, const request_t *req,
                               const bool update_cache);
static cache_obj_t *PredClock_insert(cache_t *cache, const request_t *req);
static cache_obj_t *PredClock_to_evict(cache_t *cache, const request_t *req);
static void PredClock_evict(cache_t *cache, const request_t *req);
static bool PredClock_remove(cache_t *cache, const obj_id_t obj_id);

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
cache_t *PredClock_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params) {
  cache_t *cache =
      cache_struct_init("PredClock", ccache_params, cache_specific_params);
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

  PredClock_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    PredClock_parse_params(cache, cache_specific_params);
  }

    snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "PredClock-%f",
                params->scaler);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void PredClock_free(cache_t *cache) {
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
static bool PredClock_get(cache_t *cache, const request_t *req) {
  return cache_get_base(cache, req);
}

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
static cache_obj_t *PredClock_find(cache_t *cache, const request_t *req,
                               const bool update_cache) {
  PredClock_params_t *params = (PredClock_params_t *)cache->eviction_params;
  params->vtime += 1;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (obj != NULL && update_cache) {
    if (obj->predClock.freq < params->max_freq) {
      obj->predClock.freq += 1;
    }
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
  params ->miss += 1;

  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

  obj->predClock.last_access_vtime = params->vtime;
  obj->predClock.reuse_dst = INT64_MAX;
  obj->predClock.freq = 0;
  obj->predClock.scale = 1.0;
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
  double expected_reuse_distance = (double)cache -> cache_size / miss_ratio;

  cache_obj_t *obj_to_evict = params->q_tail;
  assert(params->vtime >= obj_to_evict->predClock.last_access_vtime); // Prevent underflow
  assert((double)(params->vtime - obj_to_evict->predClock.last_access_vtime) <= DBL_MAX / params->scaler); // Prevent overflow in multiplication

  double estimated_reuse_distance = (params->vtime - obj_to_evict->predClock.last_access_vtime) * params->scaler;
  while (obj_to_evict->predClock.freq !=0 && estimated_reuse_distance <= expected_reuse_distance && obj_to_evict->predClock.check_time != params->vtime) {
    // TODO: need to discuss with jason whether we need to decay
    obj_to_evict->predClock.freq -= 1;
    params->n_obj_rewritten += 1;
    params->n_byte_rewritten += obj_to_evict->obj_size;
    move_obj_to_head(&params->q_head, &params->q_tail, obj_to_evict);
    cache->n_promotion += 1;
    obj_to_evict->predClock.check_time = params->vtime;
    obj_to_evict = params->q_tail;
    estimated_reuse_distance = (params->vtime - obj_to_evict->predClock.last_access_vtime) * params->scaler;
  }

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

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *PredClock_current_params(cache_t *cache,
                                        PredClock_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "scaler=%d\n", params->scaler);

  return params_str;
}

static void PredClock_parse_params(cache_t *cache,
                               const char *cache_specific_params) {
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
    } else {
      ERROR("%s does not have parameter %s, example parameters %s\n",
            cache->cache_name, key, PredClock_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
