//
//  RandomBelady.c
//  libCacheSim
//
//  RandomBelady eviction
//
//  Created by Juncheng on 8/2/16.
//  Copyright © 2016 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include "../../include/libCacheSim/macro.h"

#ifdef __cplusplus
extern "C" {
#endif

static const char *DEFAULT_PARAMS = "scaler=1.5";

typedef struct RandomBelady_params {
  int64_t n_miss;
  double scaler;
} RandomBelady_params_t;

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void RandomBelady_free(cache_t *cache);
static bool RandomBelady_get(cache_t *cache, const request_t *req);
static cache_obj_t *RandomBelady_find(cache_t *cache, const request_t *req, const bool update_cache);
static cache_obj_t *RandomBelady_insert(cache_t *cache, const request_t *req);
static cache_obj_t *RandomBelady_to_evict(cache_t *cache, const request_t *req);
static void RandomBelady_evict(cache_t *cache, const request_t *req);
static bool RandomBelady_remove(cache_t *cache, const obj_id_t obj_id);
static bool can_evict(cache_t *cache, const request_t *req);
static void RandomBelady_parse_params(cache_t *cache, const char *cache_specific_params);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************
/**
 * @brief initialize a RandomBelady cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params RandomBelady specific parameters, should be NULL
 */
cache_t *RandomBelady_init(const common_cache_params_t ccache_params, const char *cache_specific_params) {
  common_cache_params_t ccache_params_copy = ccache_params;
  ccache_params_copy.hashpower = MAX(12, ccache_params_copy.hashpower - 8);

  cache_t *cache = cache_struct_init("RandomBelady-2", ccache_params_copy, cache_specific_params);
  cache->cache_init = RandomBelady_init;
  cache->cache_free = RandomBelady_free;
  cache->get = RandomBelady_get;
  cache->find = RandomBelady_find;
  cache->insert = RandomBelady_insert;
  cache->to_evict = RandomBelady_to_evict;
  cache->evict = RandomBelady_evict;
  cache->remove = RandomBelady_remove;

  cache->eviction_params = my_malloc(RandomBelady_params_t);
  ((RandomBelady_params_t *)cache->eviction_params)->n_miss = 0;
  ((RandomBelady_params_t *)cache->eviction_params)->scaler = 1.5; //default

  // parse the cache specific parameters
  RandomBelady_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    RandomBelady_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "RandomBelady-%f",
            ((RandomBelady_params_t *)cache->eviction_params)->scaler);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void RandomBelady_free(cache_t *cache) { cache_struct_free(cache); }

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
static bool RandomBelady_get(cache_t *cache, const request_t *req) { return cache_get_base(cache, req); }

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
static cache_obj_t *RandomBelady_find(cache_t *cache, const request_t *req, const bool update_cache) {
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (update_cache && obj == NULL) {
    ((RandomBelady_params_t *)cache->eviction_params)->n_miss++;
  }

  if (update_cache && obj != NULL) {
    if (can_evict(cache, req)) {
      RandomBelady_remove(cache, req->obj_id);
    } else {
      obj->Random.last_access_vtime = cache->n_req;
    }
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
static cache_obj_t *RandomBelady_insert(cache_t *cache, const request_t *req) {
  if (can_evict(cache, req)) {
    return NULL;
  }

  cache_obj_t *cache_obj = cache_insert_base(cache, req);
  cache_obj->Random.last_access_vtime = cache->n_req;
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
static cache_obj_t *RandomBelady_to_evict(cache_t *cache, const request_t *req) {
  return hashtable_rand_obj(cache->hashtable);
}
// static cache_obj_t *RandomBelady_to_evict(cache_t *cache, const request_t *req) {
// #define K 16
//   cache_obj_t *lru_obj = hashtable_rand_obj(cache->hashtable), *curr_obj = NULL;

//   for (int i = 0; i < K-1; i++) {
//     curr_obj = hashtable_rand_obj(cache->hashtable);
//     if (curr_obj->Random.last_access_vtime < lru_obj->Random.last_access_vtime) {
//       lru_obj = curr_obj;
//     }
//   }
//   return lru_obj;
// }

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 */
static void RandomBelady_evict(cache_t *cache, const request_t *req) {
  cache_obj_t *obj_to_evict = RandomBelady_to_evict(cache, req);
  DEBUG_ASSERT(obj_to_evict->obj_size != 0);
  cache_evict_base(cache, obj_to_evict, true);
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
static bool RandomBelady_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }
  cache_remove_obj_base(cache, obj, true);

  return true;
}

static bool can_evict(cache_t *cache, const request_t *req) {
  if (req->next_access_vtime == INT64_MAX) {
    return true;
  }

  int64_t n_req = cache->n_req;
  int64_t n_miss = ((RandomBelady_params_t *)cache->eviction_params)->n_miss;
  double scaler = ((RandomBelady_params_t *)cache->eviction_params)->scaler;
  double miss_ratio = (double)n_miss / (double)cache->n_req;
  int64_t dist = (double)req->next_access_vtime - cache->n_req;
  int64_t threshold = ((double)cache->cache_size / miss_ratio);

  int64_t threshold_product;
  if (scaler == 0) {
    threshold_product = INT64_MAX;
  } else {
    threshold_product = threshold * scaler;
  }

  if (dist > threshold_product) {
    return true;
  } else {
    return false;
  }

  return false;
}

static void RandomBelady_parse_params(cache_t *cache,
                               const char *cache_specific_params) {
  RandomBelady_params_t *params = (RandomBelady_params_t *)cache->eviction_params;
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
      params->scaler = (float)strtod(value, &end);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "print") == 0) {
      exit(0);
    } else {
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif