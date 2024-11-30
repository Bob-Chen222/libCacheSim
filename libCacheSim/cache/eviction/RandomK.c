//
//  RandomTwo.c
//  libCacheSim
//
//  Picks two objects at random and evicts the one that is the least recently
//  used RandomTwo eviction
//
//  Created by Juncheng on 8/2/16.
//  Copyright © 2016 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include "../../include/libCacheSim/macro.h"
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct RandomK_params {
    int k;
    atomic_int_least64_t vtime;
} RandomK_params_t;

static const char *DEFAULT_PARAMS = "k=1";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void RandomK_parse_params(cache_t *cache, const char *cache_specific_params);
static void RandomK_free(cache_t *cache);
static bool RandomK_get(cache_t *cache, const request_t *req);
static cache_obj_t *RandomK_find(cache_t *cache, const request_t *req,
                                   const bool update_cache);
static cache_obj_t *RandomK_insert(cache_t *cache, const request_t *req);
static cache_obj_t *RandomK_to_evict(cache_t *cache, const request_t *req);
static void RandomK_evict(cache_t *cache, const request_t *req);
static bool RandomK_remove(cache_t *cache, const obj_id_t obj_id);
static cache_obj_t *RandomK_select(cache_t *cache, const int k);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functi ons                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************
/**
 * @brief initialize a RandomTwo cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params RandomTwo specific parameters, should be NULL
 */
cache_t *RandomK_init(const common_cache_params_t ccache_params,
                        const char *cache_specific_params) {
  common_cache_params_t ccache_params_copy = ccache_params;
  ccache_params_copy.hashpower = MAX(12, ccache_params_copy.hashpower - 8);

  cache_t *cache =
      cache_struct_init("RandomK", ccache_params_copy, cache_specific_params);
  cache->cache_init = RandomK_init;
  cache->cache_free = RandomK_free;
  cache->get = RandomK_get;
  cache->find = RandomK_find;
  cache->insert = RandomK_insert;
  cache->to_evict = RandomK_to_evict;
  cache->evict = RandomK_evict;
  cache->remove = RandomK_remove;
  pthread_spin_init(&cache->lock, 0);

  cache->eviction_params = (RandomK_params_t *) malloc(sizeof(RandomK_params_t));
  RandomK_params_t *params = (RandomK_params_t *) cache->eviction_params;
  params->k = 2;
  params->vtime = ATOMIC_VAR_INIT(0);
  if (cache_specific_params != NULL) {
    RandomK_parse_params(cache, cache_specific_params);
  }
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "Random-%d",
             params->k);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void RandomK_free(cache_t *cache) { 
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
static bool RandomK_get(cache_t *cache, const request_t *req) {
  // add one to the number of requests
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
static cache_obj_t *RandomK_find(cache_t *cache, const request_t *req,
                                   const bool update_cache) {

  //no need for blocking
  // pthread_spin_lock(&cache->lock);
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  RandomK_params_t *params = (RandomK_params_t *)cache->eviction_params;
  __atomic_fetch_add(&params->vtime, 1, __ATOMIC_RELAXED);
  if (obj != NULL && update_cache) {
    atomic_store(&obj->RandomTwo.last_access_vtime, atomic_load(&params->vtime));
  }

  // pthread_spin_unlock(&cache->lock);

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
static cache_obj_t *RandomK_insert(cache_t *cache, const request_t *req) {
  // pthread_spin_lock(&cache->lock);
  cache_obj_t *obj = cache_insert_base(cache, req);
  if (obj != NULL) {
    RandomK_params_t *params = (RandomK_params_t *)cache->eviction_params;
    atomic_store(&obj->RandomTwo.last_access_vtime, atomic_load(&params->vtime));
  }
  // pthread_spin_unlock(&cache->lock);
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
static cache_obj_t *RandomK_to_evict(cache_t *cache, const request_t *req) {
  return RandomK_select(cache, ((RandomK_params_t *)cache->eviction_params)->k);
}

//this is only a sequential version for now
static inline cache_obj_t *RandomK_select(cache_t *cache, const int k) {
  cache_obj_t *target = hashtable_rand_obj(cache->hashtable);
  DEBUG_ASSERT(target != NULL);
  for (int i = 1; i < k; i++) {
    cache_obj_t *obj = hashtable_rand_obj(cache->hashtable);
    DEBUG_ASSERT(obj != NULL && obj->obj_size != 0);
    int64_t target_v = atomic_load(&obj->RandomTwo.last_access_vtime);
    int64_t obj_v = atomic_load(&obj->RandomTwo.last_access_vtime);
    if (obj_v < target_v){
      target = obj;
    }
  }
  return target;
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 */
static void RandomK_evict(cache_t *cache, const request_t *req) {
  // pthread_spin_lock(&cache->lock);
  cache_obj_t *obj_to_evict = RandomK_to_evict(cache, req);
  // DEBUG_ASSERT(obj_to_evict != NULL);
  // DEBUG_ASSERT(obj_to_evict->obj_size != 0);
  if (obj_to_evict == NULL && cache->n_obj == 0) {
    // pthread_spin_unlock(&cache->lock);
    return;
  }
  cache_evict_base(cache, obj_to_evict, true);
  // pthread_spin_unlock(&cache->lock);
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
static bool RandomK_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }
  cache_remove_obj_base(cache, obj, true);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                parameter set up functions                     ****
// ****                                                               ****
// ***********************************************************************
static const char *RandomK_current_params(RandomK_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "k=%d", params->k);
  return params_str;
}

static void RandomK_parse_params(cache_t *cache,
                                  const char *cache_specific_params) {
  RandomK_params_t *params = (RandomK_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "k") == 0) {
      params->k = (int)strtol(value, &end, 0);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }

    } else if (strcasecmp(key, "print") == 0) {
      printf("RandomK: %s\n", RandomK_current_params(params));
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
