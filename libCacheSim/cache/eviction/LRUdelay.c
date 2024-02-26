//
//  a LRU module that supports different obj size
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
  uint64_t delay_time; // determines how often promotion is performed
  cache_obj_t *d_head;
  cache_obj_t *d_tail;

  pthread_mutex_t lock;
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

static void LRU_delay_parse_params(cache_t *cache, const char *cache_specific_params);
static void LRU_delay_free(cache_t *cache);
static bool LRU_delay_get(cache_t *cache, const request_t *req);
static cache_obj_t *LRU_delay_find(cache_t *cache, const request_t *req,
                             const bool update_cache);
static cache_obj_t *LRU_delay_insert(cache_t *cache, const request_t *req);
static cache_obj_t *LRU_delay_to_evict(cache_t *cache, const request_t *req);
static void LRU_delay_evict(cache_t *cache, const request_t *req);
static bool LRU_delay_remove(cache_t *cache, const obj_id_t obj_id);

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
cache_t *LRU_delay_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("LRU_delay", ccache_params, cache_specific_params);
  cache->cache_init = LRU_delay_init;
  cache->cache_free = LRU_delay_free;
  cache->get = LRU_delay_get;
  cache->find = LRU_delay_find;
  cache->insert = LRU_delay_insert;
  cache->evict = LRU_delay_evict;
  cache->remove = LRU_delay_remove;
  cache->to_evict = LRU_delay_to_evict;
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
  params->d_head = NULL;
  params->d_tail = NULL;
  pthread_mutex_init(&params->lock, NULL);
  cache->eviction_params = params;

  if (cache_specific_params != NULL) {
    LRU_delay_parse_params(cache, cache_specific_params);
  }


  // add d dummy nodes to the buffer
  for (int i = 0; i < params->delay_time; i++) {
    request_t *dummy_req = new_request();
    dummy_req->obj_id = 0;
    prepend_obj_to_head(&params->d_head, &params->d_tail, create_cache_obj_from_request(dummy_req));
  }
  
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "LRU_delay_%lld",
           params->delay_time);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void LRU_delay_free(cache_t *cache) { 
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
static bool LRU_delay_get(cache_t *cache, const request_t *req) {
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
static cache_obj_t *LRU_delay_find(cache_t *cache, const request_t *req,
                             const bool update_cache) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  cache_obj_t *cache_obj = cache_find_base(cache, req, update_cache);

  // no matter what, we need to check the buffer and see i
  //f there is a thing that we will reinsert

  pthread_mutex_lock(&params->lock);
  cache_obj_t* obj_reinserted = params->d_tail;
  request_t *local_req = new_request();
  copy_cache_obj_to_request(local_req, obj_reinserted);
  remove_obj_from_list(&params->d_head, &params->d_tail, obj_reinserted);

  if (obj_reinserted->obj_id != 0){
        // if not, move the object to the front
    cache_obj_t *obj = cache_find_base(cache, local_req, false);
    if (obj){
      move_obj_to_head(&params->q_head, &params->q_tail, obj);
    }
  }


  if (cache_obj && likely(update_cache)) {
      // update the buffer
      // first check if the object is already in the buffer

    prepend_obj_to_head(&params->d_head, &params->d_tail, create_cache_obj_from_request(req));
      
  }else{
    // if the object is not in the cache, we need to add a dummy object to the buffer
    request_t *dummy_req = new_request();
    dummy_req->obj_id = 0;
    prepend_obj_to_head(&params->d_head, &params->d_tail, create_cache_obj_from_request(dummy_req));
  }
  pthread_mutex_unlock(&params->lock);
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
static cache_obj_t *LRU_delay_insert(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_insert_base(cache, req);
  pthread_mutex_lock(&params->lock);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
  pthread_mutex_unlock(&params->lock);

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
static cache_obj_t *LRU_delay_to_evict(cache_t *cache, const request_t *req) {
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
static void LRU_delay_evict(cache_t *cache, const request_t *req) {
  LRU_delay_params_t *params = (LRU_delay_params_t *)cache->eviction_params;
  pthread_mutex_lock(&params->lock);
  cache_obj_t *obj_to_evict = params->q_tail;
  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
  pthread_mutex_unlock(&params->lock);
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
static void LRU_delay_remove_obj(cache_t *cache, cache_obj_t *obj_to_remove) {
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
static bool LRU_delay_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  LRU_delay_remove_obj(cache, obj);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *LRU_delay_current_params(
                                        LRU_delay_params_t *params) {
  static __thread char params_str[128];
  int n =
      snprintf(params_str, 128, "delay-time=%llu\n", params->delay_time);
  return params_str;
}

static void LRU_delay_parse_params(cache_t *cache,
                                  const char *cache_specific_params) {
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

    if (strcasecmp(key, "delay-time") == 0) {
      if (strchr(value, '.') != NULL) {
        params->delay_time = (uint64_t)((strtof(value, &end)) * cache->cache_size);
        printf("delay time is %llu\n", params->delay_time);
        if (params->delay_time == 0) {
          params->delay_time = 1;
        }
      }else{
        params->delay_time = (int)strtol(value, &end, 0);
        if (params->delay_time == 0) {
          params->delay_time = 1;
        }
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

#ifdef __cplusplus
}
#endif
