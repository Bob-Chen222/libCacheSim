//
//  lp version of lru
//  achieved through probabilistic promotion
//
//  PredProb.c
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

typedef struct PredProb_params_t {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
  float prob; // prob that the object is promoted
} PredProb_params_t;

static const char *DEFAULT_CACHE_PARAMS = "prob=0.5";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void PredProb_parse_params(cache_t *cache,
                               const char *cache_specific_params);
static void PredProb_free(cache_t *cache);
static bool PredProb_get(cache_t *cache, const request_t *req);
static cache_obj_t *PredProb_find(cache_t *cache, const request_t *req,
                             const bool update_cache);
static cache_obj_t *PredProb_insert(cache_t *cache, const request_t *req);
static cache_obj_t *PredProb_to_evict(cache_t *cache, const request_t *req);
static void PredProb_evict(cache_t *cache, const request_t *req);
static bool PredProb_remove(cache_t *cache, const obj_id_t obj_id);
static void PredProb_print_cache(const cache_t *cache);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************
/**
 * @brief initialize a PredProb cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params PredProb specific parameters, should be NULL
 */
cache_t *PredProb_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("PredProb", ccache_params, cache_specific_params);
  cache->cache_init = PredProb_init;
  cache->cache_free = PredProb_free;
  cache->get = PredProb_get;
  cache->find = PredProb_find;
  cache->insert = PredProb_insert;
  cache->evict = PredProb_evict;
  cache->remove = PredProb_remove;
  cache->to_evict = PredProb_to_evict;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->print_cache = PredProb_print_cache;

  if (ccache_params.consider_obj_metadata) {
    cache->obj_md_size = 8 * 2;
  } else {
    cache->obj_md_size = 0;
  }

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "PredProb_Belady");
#endif

  cache->eviction_params = malloc(sizeof(PredProb_params_t));
  memset(cache->eviction_params, 0, sizeof(PredProb_params_t));
  PredProb_params_t *params = cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;

  PredProb_parse_params(cache, DEFAULT_CACHE_PARAMS);
  if (cache_specific_params != NULL) {
    PredProb_parse_params(cache, cache_specific_params);
  }

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "PredProb-%.4f",
             params->prob);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void PredProb_free(cache_t *cache) { cache_struct_free(cache); }

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
static bool PredProb_get(cache_t *cache, const request_t *req) {
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
static cache_obj_t *PredProb_find(cache_t *cache, const request_t *req,
                             const bool update_cache) {
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;
  cache_obj_t *cache_obj = cache_find_base(cache, req, update_cache);

  float dice = (float)rand()/(float)(RAND_MAX); // generates random float between 0 and 1
  if (cache_obj && dice >= cache_obj -> LRUProb.scaler) {
    // cache_obj -> LRUProb.scaler = cache_obj -> LRUProb.scaler - params -> prob;
    // cache_obj -> LRUProb.freq ++;
    return cache_obj;
  }

  if (cache_obj && likely(update_cache)) {
    /* PredProb_head is the newest, move cur obj to PredProb_head */
#ifdef USE_BELADY
    if (req->next_access_vtime != INT64_MAX)
#endif
      move_obj_to_head(&params->q_head, &params->q_tail, cache_obj);
      cache -> n_promotion ++;
      cache_obj -> LRUProb.scaler = MAX(cache_obj -> LRUProb.scaler * params -> prob, 0.1);
      cache_obj -> LRUProb.freq ++;
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
static cache_obj_t *PredProb_insert(cache_t *cache, const request_t *req) {
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;

  cache_obj_t *obj = cache_insert_base(cache, req);
  obj->LRUProb.freq = 0;
  obj->LRUProb.scaler = 1.0;
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
static cache_obj_t *PredProb_to_evict(cache_t *cache, const request_t *req) {
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;

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
static void PredProb_evict(cache_t *cache, const request_t *req) {
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  DEBUG_ASSERT(params->q_tail != NULL);

  // we can simply call remove_obj_from_list here, but for the best performance,
  // we chose to do it manually
  // remove_obj_from_list(&params->q_head, &params->q_tail, obj)

  params->q_tail = params->q_tail->queue.prev;
  if (likely(params->q_tail != NULL)) {
    params->q_tail->queue.next = NULL;
  } else {
    /* cache->n_obj has not been updated */
    DEBUG_ASSERT(cache->n_obj == 1);
    params->q_head = NULL;
  }

#if defined(TRACK_DEMOTION)
  if (cache->track_demotion)
  printf("%ld demote %ld %ld\n", cache->n_req, obj_to_evict->create_time,
         obj_to_evict->misc.next_access_vtime);
#endif

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
static void PredProb_remove_obj(cache_t *cache, cache_obj_t *obj) {
  assert(obj != NULL);

  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;

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
static bool PredProb_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;

  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
  cache_remove_obj_base(cache, obj, true);

  return true;
}

static void PredProb_print_cache(const cache_t *cache) {
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;
  cache_obj_t *cur = params->q_head;
  // print from the most recent to the least recent
  if (cur == NULL) {
    printf("empty\n");
    return;
  }
  while (cur != NULL) {
    printf("%lu->", cur->obj_id);
    cur = cur->queue.next;
  }
  printf("END\n");
}

// ***********************************************************************
// ****                                                               ****
// ****                parameter set up functions                     ****
// ****                                                               ****
// ***********************************************************************
static const char *PredProb_current_params(PredProb_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "n-seg=%f\n", params->prob);
  return params_str;
}

static void PredProb_parse_params(cache_t *cache,
                                const char *cache_specific_params) {
  PredProb_params_t *params = (PredProb_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "prob") == 0) {
      params->prob = (float)strtod(value, NULL);
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", PredProb_current_params(params));
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
