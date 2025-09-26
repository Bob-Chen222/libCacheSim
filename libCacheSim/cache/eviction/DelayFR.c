//
//  DelayFR, the same as FIFO-Reinsertion or second chance, is a FIFO with
//  which inserts back some objects upon eviction
//
//
//  DelayFR.c
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include <stdio.h>

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY
#undef USE_BELADY

static const char *DEFAULT_PARAMS = "n-bit-counter=1,delay-ratio=0.05";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void DelayFR_parse_params(cache_t *cache, const char *cache_specific_params);
static void DelayFR_free(cache_t *cache);
static bool DelayFR_get(cache_t *cache, const request_t *req);
static cache_obj_t *DelayFR_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *DelayFR_insert(cache_t *cache, const request_t *req);
static cache_obj_t *DelayFR_to_evict(cache_t *cache, const request_t *req);
static void DelayFR_evict(cache_t *cache, const request_t *req);
static bool DelayFR_remove(cache_t *cache, const obj_id_t obj_id);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief initialize a DelayFR cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params DelayFR specific parameters as a string
 */
cache_t *DelayFR_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("DelayFR", ccache_params, cache_specific_params);
  cache->cache_init = DelayFR_init;
  cache->cache_free = DelayFR_free;
  cache->get = DelayFR_get;
  cache->find = DelayFR_find;
  cache->insert = DelayFR_insert;
  cache->evict = DelayFR_evict;
  cache->remove = DelayFR_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = DelayFR_to_evict;
  cache->obj_md_size = 0;
  cache->num_stats = 0;
  cache->num_stats2 = 0;
  cache->num_stats3 = 0;
  cache->num_stats4 = 0;
  cache->num_stats5 = 0;

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "DelayFR_Belady");
#endif

  cache->eviction_params = malloc(sizeof(DelayFR_params_t));
  memset(cache->eviction_params, 0, sizeof(DelayFR_params_t));
  DelayFR_params_t *params = (DelayFR_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->n_bit_counter = 1;
  params->max_freq = 1;

  DelayFR_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    DelayFR_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "DelayFR-%d-%f", params->n_bit_counter, params->delay_ratio);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void DelayFR_free(cache_t *cache) {
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
static bool DelayFR_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

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
static cache_obj_t *DelayFR_find(cache_t *cache, const request_t *req, const bool update_cache) {
  DelayFR_params_t *params = (DelayFR_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (obj != NULL && update_cache && obj->delay_FR.freq < params->max_freq) {
    uint64_t time_passed = params->current_time - obj->delay_FR.last_reuse_time;
    if (time_passed > params->delay_time) {
      obj->delay_FR.freq += 1;
      obj->delay_FR.last_reuse_time = params->current_time;
    }
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
static cache_obj_t *DelayFR_insert(cache_t *cache, const request_t *req) {
  DelayFR_params_t *params = (DelayFR_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

  params->current_time += 1;

  obj->delay_FR.freq = 0;
  obj->delay_FR.last_reuse_time = params->current_time;

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
static cache_obj_t *DelayFR_to_evict(cache_t *cache, const request_t *req) { ERROR("to_evict not supported"); }

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 * @param evicted_obj if not NULL, return the evicted object to caller
 */
static void DelayFR_evict(cache_t *cache, const request_t *req) {
  DelayFR_params_t *params = (DelayFR_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  if (obj_to_evict->delay_FR.freq == 0) {
    remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
    cache_evict_base(cache, obj_to_evict, true);
    return;
  }

  obj_to_evict->delay_FR.freq -= 1;
  move_obj_to_head(&params->q_head, &params->q_tail, obj_to_evict);

  params->n_obj_rewritten += 1;
  params->n_byte_rewritten += obj_to_evict->obj_size;

  cache->n_promotion += 1;
  params->current_time += 1;
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
static void DelayFR_remove_obj(cache_t *cache, cache_obj_t *obj) {
  DelayFR_params_t *params = (DelayFR_params_t *)cache->eviction_params;

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
static bool DelayFR_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  DelayFR_remove_obj(cache, obj);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *DelayFR_current_params(cache_t *cache, DelayFR_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "n-bit-counter=%d,delay-ratio=%f\n", params->n_bit_counter, params->delay_ratio);

  return params_str;
}

static void DelayFR_parse_params(cache_t *cache, const char *cache_specific_params) {
  DelayFR_params_t *params = (DelayFR_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "n-bit-counter") == 0) {
      params->n_bit_counter = (int)strtol(value, &end, 0);
      params->max_freq = (1 << params->n_bit_counter) - 1;
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "delay-ratio") == 0) {
      params->delay_ratio = strtod(value, &end);
      params->delay_time = params->delay_ratio * cache->cache_size;
      // INFO("delay_time: %" PRIu64 "\n", params->delay_time);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", DelayFR_current_params(cache, params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s, example parameters %s\n", cache->cache_name, key,
            DelayFR_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
