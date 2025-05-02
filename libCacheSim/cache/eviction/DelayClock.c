//
//  DelayClock, use the last access age and the last promote age to decide whether to promote
//  which inserts back some objects upon eviction
//
//
//  Clock.c
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY
#undef USE_BELADY

static const char *DEFAULT_PARAMS = "n-bit-counter=1,decrease-rate=1,scale=1.0";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void DelayClock_parse_params(cache_t *cache, const char *cache_specific_params);
static void DelayClock_free(cache_t *cache);
static bool DelayClock_get(cache_t *cache, const request_t *req);
static cache_obj_t *DelayClock_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *DelayClock_insert(cache_t *cache, const request_t *req);
static cache_obj_t *DelayClock_to_evict(cache_t *cache, const request_t *req);
static void DelayClock_evict(cache_t *cache, const request_t *req);
static bool DelayClock_remove(cache_t *cache, const obj_id_t obj_id);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief initialize a Clock cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params Clock specific parameters as a string
 */
cache_t *DelayClock_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("DelayClock", ccache_params, cache_specific_params);
  cache->cache_init = DelayClock_init;
  cache->cache_free = DelayClock_free;
  cache->get = DelayClock_get;
  cache->find = DelayClock_find;
  cache->insert = DelayClock_insert;
  cache->evict = DelayClock_evict;
  cache->remove = DelayClock_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = DelayClock_to_evict;
  cache->obj_md_size = 0;
  cache->num_stats = 0;
  cache->num_stats2 = 0;
  cache->num_stats3 = 0;
  cache->num_stats4 = 0;
  cache->num_stats5 = 0;

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "Clock_Belady");
#endif

  cache->eviction_params = malloc(sizeof(Clock_params_t));
  memset(cache->eviction_params, 0, sizeof(Clock_params_t));
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->n_bit_counter = 1;
  params->max_freq = 1;
  params->decrease_rate = 1;
  params->scale = 1.0f;

  DelayClock_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    DelayClock_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "DelayClock-%d-%d-%d-%f", params->n_bit_counter, params->decrease_rate, cache->version_num + 1
  , params->scale);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void DelayClock_free(cache_t *cache) {
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
static bool DelayClock_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

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
static cache_obj_t *DelayClock_find(cache_t *cache, const request_t *req, const bool update_cache) {
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (obj != NULL && update_cache) {
    obj->last_access_time = cache->n_req;
    obj->last_access_itime = cache->n_insert;
    obj->clock.num_hits += 1;
    obj->clock.next_access_vtime = req->next_access_vtime;
    if (obj->clock.freq < params->max_freq) {
      obj->clock.freq += 1;
    }
    obj->is_promoted = false;
    if (UINT64_MAX != cache -> time_downgrade[cache -> n_req]){
      obj->clock.freq = 0;
      obj->last_access_itime = 0;
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
static cache_obj_t *DelayClock_insert(cache_t *cache, const request_t *req) {
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);

  obj->clock.freq = 0;
  obj->last_access_time = cache->n_req;
  obj->last_access_itime = 0;
  obj->last_promote_itime = 0;
  obj->last_promote_time = 0;
  obj->clock.num_hits = 0;
  obj->clock.next_access_vtime = req->next_access_vtime;
  obj->is_promoted = false;
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
static cache_obj_t *DelayClock_to_evict(cache_t *cache, const request_t *req) {
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;

  int n_round = 0;
  cache_obj_t *obj_to_evict = params->q_tail;
#ifdef USE_BELADY
  while (obj_to_evict->next_access_vtime != INT64_MAX) {
#else
  while (obj_to_evict->clock.freq - n_round >= 1) {
#endif
    obj_to_evict = obj_to_evict->queue.prev;
    if (obj_to_evict == NULL) {
      obj_to_evict = params->q_tail;
      n_round += 1;
    }
  }

  return obj_to_evict;
}

static promote(cache_t *cache, cache_obj_t *obj) {
    Clock_params_t *params = (Clock_params_t *)cache->eviction_params;
    int64_t access_iage = cache->n_insert - obj->last_access_itime;
    int64_t promote_iage = cache->n_insert - obj->last_promote_itime;
    int64_t promote_vage = cache->n_req - obj->last_promote_time;
    if (((double)access_iage / (double)promote_iage) < params->scale) {

      // if (access_iage < promote_iage){
      //   printf("promote, access_iage=%ld, promote_iage=%ld, future access vage=%ld, promote_vage=%ld\n",
      //           access_iage, promote_iage, obj->clock.next_access_vtime - cache->n_req, promote_vage);
      // }
      return true;
    }else{
      if (obj->clock.next_access_vtime != INT64_MAX && promote_iage != access_iage) {
        // printf("evicted, access_iage=%ld, promote_iage=%ld, future access vage=%ld, promote_vage=%ld\n",
        //       access_iage, promote_iage, obj->clock.next_access_vtime - cache->n_req, promote_vage);
      }
      return false;
    }
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
static void DelayClock_evict(cache_t *cache, const request_t *req) {
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;

  cache_obj_t *obj_to_evict = params->q_tail;
  int64_t obj_id_front = obj_to_evict->obj_id;
  // promote(cache, obj_to_evict)
    while (obj_to_evict->clock.freq >= 0 && promote(cache, obj_to_evict)) {
    obj_to_evict->clock.freq -= params->decrease_rate;
    params->n_obj_rewritten += 1;
    params->n_byte_rewritten += obj_to_evict->obj_size;
    move_obj_to_head(&params->q_head, &params->q_tail, obj_to_evict);
    cache->n_promotion += 1;
    obj_to_evict->last_promote_itime = cache->n_insert;
    obj_to_evict->last_promote_time = cache->n_req;
    // obj_to_evict->last_access_itime = 0; (maybe useful move)
    obj_to_evict->is_promoted = true;
    obj_to_evict = params->q_tail;
  }

  if (obj_to_evict->is_promoted){
    // that means the promotion failed
    cache->time_downgrade[obj_to_evict->last_access_time] = cache->version_num + 1;
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
static void DelayClock_remove_obj(cache_t *cache, cache_obj_t *obj) {
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;

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
static bool DelayClock_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  DelayClock_remove_obj(cache, obj);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *DelayClock_current_params(cache_t *cache, Clock_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "n-bit-counter=%d\n", params->n_bit_counter);

  return params_str;
}

static void DelayClock_parse_params(cache_t *cache, const char *cache_specific_params) {
  Clock_params_t *params = (Clock_params_t *)cache->eviction_params;
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
    } else if (strcasecmp(key, "decrease-rate") == 0) {
      params->decrease_rate = (int)strtol(value, &end, 0);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "scale") == 0){
      params->scale = strtod(value, &end);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    }
    else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", DelayClock_current_params(cache, params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s, example paramters %s\n", cache->cache_name, key,
            DelayClock_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif