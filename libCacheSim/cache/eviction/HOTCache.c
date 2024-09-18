//
//  Quick demotion + lazy promotion v2
//
//  FIFO + Clock
//  the ratio of FIFO is decided dynamically
//  based on the marginal hits on FIFO-ghost and main cache
//  we track the hit distribution of FIFO-ghost and main cache
//  if the hit distribution of FIFO-ghost at pos 0 is larger than
//  the hit distribution of main cache at pos -1,
//  we increase FIFO size by 1
//
//
//  HOTCache.c
//  libCacheSim
//
//  Created by Juncheng on 1/24/23
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"
#include "../../dataStructure/pqueue.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
//   initial selection
    cache_t *main_cache;
    uint64_t *buffer; //a buffer that stores only 5 to 10 objects depend on the performance and will be setup in the init function
    pqueue_t *pq; //a priority queue for selecting the object to be evicted
    uint64_t buffer_size;

    // main cache
    char main_cache_type[32];

    bool init; //whether we are still choosing the selection

    // profiling
    int found_in_buffer;
} HOTCache_params_t;

static const char *DEFAULT_CACHE_PARAMS =
    "main-cache=Clock";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************
cache_t *HOTCache_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);
static void HOTCache_free(cache_t *cache);
static bool HOTCache_get(cache_t *cache, const request_t *req);

static cache_obj_t *HOTCache_find(cache_t *cache, const request_t *req,
                                const bool update_cache);
static cache_obj_t *HOTCache_insert(cache_t *cache, const request_t *req);
static cache_obj_t *HOTCache_to_evict(cache_t *cache, const request_t *req);
static void HOTCache_evict(cache_t *cache, const request_t *req);
static bool HOTCache_remove(cache_t *cache, const obj_id_t obj_id);
static inline int64_t HOTCache_get_occupied_byte(const cache_t *cache);
static inline int64_t HOTCache_get_n_obj(const cache_t *cache);
static inline bool HOTCache_can_insert(cache_t *cache, const request_t *req);
static void HOTCache_parse_params(cache_t *cache,
                                const char *cache_specific_params);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

cache_t *HOTCache_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("HOTCache", ccache_params, cache_specific_params);
  cache->cache_init = HOTCache_init;
  cache->cache_free = HOTCache_free;
  cache->get = HOTCache_get;
  cache->find = HOTCache_find;
  cache->insert = HOTCache_insert;
  cache->evict = HOTCache_evict;
  cache->remove = HOTCache_remove;
  cache->to_evict = HOTCache_to_evict;
  cache->get_n_obj = HOTCache_get_n_obj;
  cache->get_occupied_byte = HOTCache_get_occupied_byte;
  cache->can_insert = HOTCache_can_insert;

  cache->obj_md_size = 0;

  cache->eviction_params = malloc(sizeof(HOTCache_params_t));
  memset(cache->eviction_params, 0, sizeof(HOTCache_params_t));
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;

  HOTCache_parse_params(cache, DEFAULT_CACHE_PARAMS);
  if (cache_specific_params != NULL) {
    HOTCache_parse_params(cache, cache_specific_params);
  }

  int64_t main_cache_size = ccache_params.cache_size;

  common_cache_params_t ccache_params_local = ccache_params;
  ccache_params_local.cache_size = main_cache_size;
  if (strcasecmp(params->main_cache_type, "FIFO") == 0) {
    params->main_cache = FIFO_init(ccache_params_local, NULL);
  } else if (strcasecmp(params->main_cache_type, "clock") == 0) {
    printf("HOTCache: using clock\n");
    params->main_cache = Clock_init(ccache_params_local, NULL);
  } else if (strcasecmp(params->main_cache_type, "clock2") == 0) {
    params->main_cache = Clock_init(ccache_params_local, "n-bit-counter=2");
  } else if (strcasecmp(params->main_cache_type, "clock3") == 0) {
    params->main_cache = Clock_init(ccache_params_local, "n-bit-counter=3");
  } else if (strcasecmp(params->main_cache_type, "lru") == 0) {
    params->main_cache = LRU_init(ccache_params_local, NULL);
  } else if (strcasecmp(params->main_cache_type, "lruprob") == 0) {
    params->main_cache = lpLRU_prob_init(ccache_params_local, "prob=0.5");
  } else if (strcasecmp(params->main_cache_type, "lrudelay") == 0) {
    params->main_cache = LRU_delay_init(ccache_params_local, "delay-time=0.3");
  } else{
    ERROR("HOTCache: main cache type %s is not supported\n",
          params->main_cache_type);
  }

  params->buffer_size = 10;
  params->buffer = malloc(sizeof(cache_obj_t *) * params->buffer_size);
  params->pq = pqueue_init((unsigned long)8e6);
  params->init = true;
  params->found_in_buffer = 0;

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "HOTCache-%s",
           params->main_cache_type);
  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void HOTCache_free(cache_t *cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  params->main_cache->cache_free(params->main_cache);
  printf("HOTCache: found in buffer %d\n", params->found_in_buffer);
  free(params->buffer);
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
static bool HOTCache_get(cache_t *cache, const request_t *req) {
  // check if the cache is full
  bool cache_hit = cache_get_base(cache, req);
  return cache_hit;
}

// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************
/**
 * @brief find an object in the cache
 *
 * @param cache
 * @param req
 * @param update_cache whether to update the cache,
 *  if true, the object is promoted
 *  and if the object is expired, it is removed from the cache
 * @return the object or NULL if not found
 */
static cache_obj_t *HOTCache_find(cache_t *cache, const request_t *req,
                                const bool update_cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  cache_obj_t *cached_obj = NULL;
  
  // if update cache is false, we only check the fifo and main caches
  DEBUG_ASSERT(update_cache == true); // only support this mode
  if (params -> init){
    // if the cache is in the init phase, we need to run the main cache
    cached_obj = params -> main_cache->find(params->main_cache, req, update_cache);
    assert(cached_obj == params -> main_cache -> find(params -> main_cache, req, update_cache));
    if (cached_obj == NULL) return NULL;
    cached_obj->misc.freq++;
    pqueue_pri_t pri = {.pri = cached_obj->misc.freq};
    pqueue_change_priority(params->pq, pri, cached_obj->misc.pq_node);
    return cached_obj;
  }else{
    // check the buffer
    for (int i = 0; i < params -> buffer_size; i++){
      if (params -> buffer[i] == req -> obj_id){
        cached_obj = malloc(sizeof(cache_obj_t));
        copy_request_to_cache_obj(cached_obj, req);
        params -> found_in_buffer++;
        return cached_obj;
      }
    }
    // check the main cache
    cached_obj = params->main_cache->find(params->main_cache, req, update_cache);
    return cached_obj;
  }

  return NULL; // should not reach here
}

/**
 * @brief insert an object into the cache,
 * update the hash table and cache metadata
 * this function assumes the cache has enough space
 * eviction should be
 * performed before calling this function
 *
 * @param cache
 * @param req
 * @return the inserted object
 */
static cache_obj_t *HOTCache_insert(cache_t *cache, const request_t *req) {
  // do the regular insertion
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  DEBUG_ASSERT(!hashtable_find_obj_id(params->main_cache->hashtable, req->obj_id));
  cache_obj_t *obj = params->main_cache->insert(params->main_cache, req);
  // // also need to insert the object into the base
  // // cache_insert_base(cache, req);
  obj -> last_access_time = cache -> n_insert;
  cache->occupied_byte +=
      (int64_t)obj->obj_size + (int64_t)cache->obj_md_size;
  cache->n_obj += 1;
  if (cache -> n_obj + 1 == cache -> cache_size && params -> init){
    // we have enough objects to select
    printf("HOTCache: initialization done\n");
    params -> init = false;
    // we select top 10 objects from the priority queue to the buffer
    for (int i = 0; i < params->buffer_size; i++){
      pq_node_t *node = (pq_node_t *)pqueue_pop(params->pq);
      params -> buffer[i] = node -> obj_id;
      printf("selected obj id: %lu\n", node -> obj_id);
      // we should also delete these objects in the main cache

    }
    // free the priority queue
    pq_node_t *node = pqueue_pop(params->pq);
    while (node) {
      my_free(sizeof(pq_node_t), node);
      node = pqueue_pop(params->pq);
    }
    pqueue_free(params->pq);
  }

  if (params -> init){
    // only when initalizing the cache, we need to
    pq_node_t *node = malloc(sizeof(pq_node_t));
    node -> obj_id = obj -> obj_id;
    //priority is ranked based on the frequency
    node -> pri.pri = 0;
    pqueue_insert(params->pq, (void*)node);
    // we need to make sure that this is not in the buffer

    obj -> misc.pq_node = node;
    obj -> misc.freq = 0;
  }
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
static cache_obj_t *HOTCache_to_evict(cache_t *cache, const request_t *req) {
  assert(false);
  return NULL;
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
static void HOTCache_evict(cache_t *cache, const request_t *req) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  params->main_cache->evict(params->main_cache, req);
  cache->occupied_byte -= 1; //assert occupied byte is 1
  cache->n_obj -= 1;
  // everytime we update the n_promotion
  cache -> n_promotion = params -> main_cache -> n_promotion;
  return;
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
static bool HOTCache_remove(cache_t *cache, const obj_id_t obj_id) {
  assert(false);
  return false;
  // HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  // bool removed = false;
  // removed = removed || params->fifo->remove(params->fifo, obj_id);
  // removed = removed || params->fifo_ghost->remove(params->fifo_ghost, obj_id);
  // removed = removed || params->main_cache->remove(params->main_cache, obj_id);

  // return removed;
}

static inline int64_t HOTCache_get_occupied_byte(const cache_t *cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  return params->main_cache->get_occupied_byte(params->main_cache);
}

static inline int64_t HOTCache_get_n_obj(const cache_t *cache) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;
  return params->main_cache->get_n_obj(params->main_cache);
}

static inline bool HOTCache_can_insert(cache_t *cache, const request_t *req) {
  HOTCache_params_t *params = (HOTCache_params_t *)cache->eviction_params;

  return req->obj_size <= params->main_cache->cache_size;
}

// ***********************************************************************
// ****                                                               ****
// ****                parameter set up functions                     ****
// ****                                                               ****
// ***********************************************************************
static const char *HOTCache_current_params(HOTCache_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "main-cache=%s\n",
           params->main_cache->cache_name);
  return params_str;
}

static void HOTCache_parse_params(cache_t *cache,
                                const char *cache_specific_params) {
  HOTCache_params_t *params = (HOTCache_params_t *)(cache->eviction_params);

  char *params_str = strdup(cache_specific_params);
  char *old_params_str = params_str;
  // char *end;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }

    if (strcasecmp(key, "main-cache") == 0) {
      strncpy(params->main_cache_type, value, 30);
    } else if (strcasecmp(key, "print") == 0) {
      printf("parameters: %s\n", HOTCache_current_params(params));
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
