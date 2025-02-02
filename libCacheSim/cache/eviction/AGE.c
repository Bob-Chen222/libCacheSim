//
// The newest implementation of AGE eviction that only prevent recent promotions of objects
// dist_ratio is used for controlling the 
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

static const char *DEFAULT_PARAMS = "dist-ratio=0.1";
static int false_negative = 0;
static int total_negative = 0;

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void AGE_parse_params(cache_t *cache, const char *cache_specific_params);
static void AGE_free(cache_t *cache);
static bool AGE_get(cache_t *cache, const request_t *req);
static cache_obj_t *AGE_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *AGE_insert(cache_t *cache, const request_t *req);
static cache_obj_t *AGE_to_evict(cache_t *cache, const request_t *req);
static void AGE_evict(cache_t *cache, const request_t *req);
static bool AGE_remove(cache_t *cache, const obj_id_t obj_id);
static bool is_retained(cache_t *cache, cache_obj_t *obj_to_evict, const double expected_reuse_distance);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief initialize a AGE cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params AGE specific parameters as a string
 */
cache_t *AGE_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("AGE", ccache_params, cache_specific_params);
  cache->cache_init = AGE_init;
  cache->cache_free = AGE_free;
  cache->get = AGE_get;
  cache->find = AGE_find;
  cache->insert = AGE_insert;
  cache->evict = AGE_evict;
  cache->remove = AGE_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = AGE_to_evict;
  cache->obj_md_size = 0;

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "AGE_Belady");
#endif

  cache->eviction_params = malloc(sizeof(AGE_params_t));
  memset(cache->eviction_params, 0, sizeof(AGE_params_t));
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->n_bit_counter = 1;
  params->max_freq = 1;
  params->counter_insert = 0;

  AGE_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    AGE_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "AGE-%f", params->dist_ratio);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void AGE_free(cache_t *cache) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;
  double fn_rate = (double) false_negative / (double) total_negative;
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
static bool AGE_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

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
static cache_obj_t *AGE_find(cache_t *cache, const request_t *req, const bool update_cache) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;
  params->vtime += 1;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (obj != NULL && update_cache) {
    if (obj->age.freq < params->max_freq) {
      // added a threshold!!
      double threshold = params -> dist_ratio;
      if (params->counter_insert - obj->age.pos >= threshold * cache -> cache_size) {
        obj->age.freq += 1; 
      }
    }
    obj->age.last_access_vtime = params->vtime;
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
static cache_obj_t *AGE_insert(cache_t *cache, const request_t *req) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;
  params->miss += 1;
  params->counter_insert += 1;

  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

  obj->age.last_access_vtime = params->vtime;
  obj->age.freq = 0;
  obj->age.pos = params->counter_insert;
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
static cache_obj_t *AGE_to_evict(cache_t *cache, const request_t *req) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;

  int n_round = 0;
  cache_obj_t *obj_to_evict = params->q_tail;
#ifdef USE_BELADY
  while (obj_to_evict->next_access_vtime != INT64_MAX) {
#else
  while (obj_to_evict->age.freq - n_round >= 1) {
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
static void AGE_evict(cache_t *cache, const request_t *req) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;
  double miss_ratio;
  miss_ratio = (double)params->miss / (double)params->vtime;
  double expected_reuse_distance = (double)cache->cache_size / miss_ratio;

  cache_obj_t *obj_to_evict = params->q_tail;

//   bool retained = is_retained(cache, obj_to_evict, expected_reuse_distance);
  while (obj_to_evict->age.freq > 0 && obj_to_evict->age.check_time != params->vtime) {
    params->counter_insert += 1;
    obj_to_evict->age.freq -= 1;
    params->n_obj_rewritten += 1;
    params->n_byte_rewritten += obj_to_evict->obj_size;
    move_obj_to_head(&params->q_head, &params->q_tail, obj_to_evict);
    cache->n_promotion += 1;
    obj_to_evict->age.check_time = params->vtime;
    obj_to_evict->age.pos = params->counter_insert;
    obj_to_evict = params->q_tail;
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
static void AGE_remove_obj(cache_t *cache, cache_obj_t *obj) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;

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
static bool AGE_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  AGE_remove_obj(cache, obj);

  return true;
}


// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *AGE_current_params(cache_t *cache, AGE_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "dist-ratio=%d\n", params->dist_ratio);

  return params_str;
}

static void AGE_parse_params(cache_t *cache, const char *cache_specific_params) {
  AGE_params_t *params = (AGE_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "dist-ratio") == 0) {
      params->n_bit_counter = 1;
      params->dist_ratio = (float)strtod(value, &end);
      params->max_freq = 1;
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", AGE_current_params(cache, params));
      exit(0);
    }
    else {
      ERROR("%s does not have parameter %s, example parameters %s\n", cache->cache_name, key,
            AGE_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
