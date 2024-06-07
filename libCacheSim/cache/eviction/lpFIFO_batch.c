//
//  Batch-reinsertion modified based on Clock
//  Does not promote at eviction time, but periodically based on 
//  the provided constant param batch-size
//
//  lpFIFO_batch.c
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

typedef struct {
  cache_obj_t *q_head;
  cache_obj_t *q_tail;
  uint64_t batch_size; // determines how often promotion is performed
  float promotion_ratio; // determines how many objects are promoted
  uint64_t *buffer; //a large buffer is fine because it still isolated each thread's access
  uint64_t num_thread; // will always be 1
  uint64_t buffer_pos;

  uint64_t prev_promote_time;
  uint64_t time_insert;
} lpFIFO_batch_params_t;

static const char *DEFAULT_PARAMS = "batch-size=0.2";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void lpFIFO_print(cache_t *cache);
static void lpFIFO_batch_parse_params(cache_t *cache,
                               const char *cache_specific_params);
static void lpFIFO_batch_free(cache_t *cache);
static bool lpFIFO_batch_get(cache_t *cache, const request_t *req);
static cache_obj_t *lpFIFO_batch_find(cache_t *cache, const request_t *req,
                               const bool update_cache);
static cache_obj_t *lpFIFO_batch_insert(cache_t *cache, const request_t *req);
static cache_obj_t *lpFIFO_batch_to_evict(cache_t *cache, const request_t *req);
static void lpFIFO_batch_evict(cache_t *cache, const request_t *req);
static void lpFIFO_batch_promote_all(cache_t *cache, const request_t *req, uint64_t *buff, const uint64_t* batch_size);
static bool lpFIFO_batch_remove(cache_t *cache, const obj_id_t obj_id);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief initialize a lpFIFO_batch cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params lpFIFO_batch specific parameters as a string
 */
cache_t *lpFIFO_batch_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("lpFIFO_batch", ccache_params, cache_specific_params);
  cache->cache_init = lpFIFO_batch_init;
  cache->cache_free = lpFIFO_batch_free;
  cache->get = lpFIFO_batch_get;
  cache->find = lpFIFO_batch_find;
  cache->insert = lpFIFO_batch_insert;
  cache->evict = lpFIFO_batch_evict;
  cache->remove = lpFIFO_batch_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = lpFIFO_batch_to_evict;
  cache->obj_md_size = 0;

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "lpFIFO_batch_Belady");
#endif

  cache->eviction_params = malloc(sizeof(lpFIFO_batch_params_t));
  memset(cache->eviction_params, 0, sizeof(lpFIFO_batch_params_t));
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->batch_size = 10000;
  params->num_thread = 1;
  params->buffer_pos = 0;

  params->prev_promote_time = 0;
  params->time_insert = 0;

  lpFIFO_batch_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    lpFIFO_batch_parse_params(cache, cache_specific_params);
  }

  common_cache_params_t ccache_params_local = ccache_params;
  if (params->batch_size == 0) {
    params->batch_size = 1;
  }
  ccache_params_local.cache_size = params->batch_size;
  if (ccache_params_local.cache_size == 0) {
    ccache_params_local.cache_size = 1;
  }

  params->buffer = malloc(sizeof(uint64_t) * params->batch_size);

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "lpFIFO_batch-%f",
             params->promotion_ratio);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void lpFIFO_batch_free(cache_t *cache) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)(cache->eviction_params);
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
static bool lpFIFO_batch_get(cache_t *cache, const request_t *req) {
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
 *  if true, the object is stored to be promoted in buffer later
 *  and if the object is expired, it is removed from the cache
 * @return true on hit, false on miss
 */
static cache_obj_t *lpFIFO_batch_find(cache_t *cache, const request_t *req,
                               const bool update_cache) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);

  if (obj != NULL && update_cache) {
    // atomic add 1 and then read the value
    params->buffer[params->buffer_pos % params->batch_size] = obj -> obj_id;
    params->buffer_pos += 1;
    if (params->time_insert - params->prev_promote_time >= params -> batch_size){
      lpFIFO_batch_promote_all(cache, req, params->buffer, &params -> batch_size);
      params->prev_promote_time = params->time_insert;
      params->buffer_pos = 0;
    }
    // if (params->buffer_pos == params -> batch_size){
    //   lpFIFO_batch_promote_all(cache, req, params->buffer);
    //   params->buffer_pos = 0;
    // }
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
static cache_obj_t *lpFIFO_batch_insert(cache_t *cache, const request_t *req) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;
  params->time_insert += 1;
  cache_obj_t *obj = cache_insert_base(cache, req);
  prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
  params->buffer_pos += 1;

  if (params->buffer_pos == params->batch_size){
    lpFIFO_batch_promote_all(cache, req, params->buffer);
    params->buffer_pos = 0;
  }
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
static cache_obj_t *lpFIFO_batch_to_evict(cache_t *cache, const request_t *req) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;

  int n_round = 0;
  cache_obj_t *obj_to_evict = params->q_tail;
#ifdef USE_BELADY
  while (obj_to_evict->next_access_vtime != INT64_MAX) {
#else
  while (obj_to_evict->lpFIFO_batch.freq - n_round >= 1) {
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
 * No longer promotes upon eviction for marked objects
 *
 * @param cache
 * @param req not used
 * @param evicted_obj if not NULL, return the evicted object to caller
 */
static void lpFIFO_batch_evict(cache_t *cache, const request_t *req) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;

  cache_obj_t *obj_to_evict = params->q_tail;
  remove_obj_from_list(&params->q_head, &params->q_tail, obj_to_evict);
  cache_evict_base(cache, obj_to_evict, true);
}

/**
 * @brief promotes all currently marked objects within the cache
 *
 * @param cache
 * @param req not used
 */
static void lpFIFO_batch_promote_all(cache_t *cache, const request_t *req, uint64_t *buff, const uint64_t* start) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;
  uint64_t pos = 0;
  uint64_t count = 0;
  if (*start > params->batch_size) {
    pos = *start;
    count = params->batch_size;
  }else{
    count = *start;
  }
  uint64_t obj_to_promote = 0L;

  for (int i = 0; i < count; i++) {
    obj_to_promote = buff[pos % params->batch_size];
    cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_to_promote);
    if (obj != NULL) {
      move_obj_to_head(&params->q_head, &params->q_tail, obj);
    }
    pos += 1;
  }

  // int count = 0;
  // cache_obj_t *obj_to_promote = NULL;
  // for (obj_to_promote = buff[count]; count < params->batch_size; obj_to_promote = buff[count]) {
  //   // copy_cache_obj_to_request(local_req, obj_to_promote);
  //   cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_to_promote->obj_id);
  //   if (obj != NULL) {
  //     move_obj_to_head(&params->q_head, &params->q_tail, obj);
  //   }
  //   count += 1;
  // }

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
static void lpFIFO_batch_remove_obj(cache_t *cache, cache_obj_t *obj) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;

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
static bool lpFIFO_batch_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  lpFIFO_batch_remove_obj(cache, obj);

  return true;
}

static void lpFIFO_print(cache_t *cache) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;
  cache_obj_t *cur = params->q_head;
  // print from the most recent to the least recent
  if (cur == NULL) {
    printf("empty\n");
  }
  while (cur != NULL) {
    printf("%ld->", cur->obj_id);
    cur = cur->queue.next;
  }
  printf("\n");
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *lpFIFO_batch_current_params(cache_t *cache,
                                        lpFIFO_batch_params_t *params) {
  static __thread char params_str[128];
  int n =
      snprintf(params_str, 128, "not set currently\n");

  return params_str;
}

static void lpFIFO_batch_parse_params(cache_t *cache,
                               const char *cache_specific_params) {
  lpFIFO_batch_params_t *params = (lpFIFO_batch_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "batch-size") == 0) {
      if (strchr(value, '.') != NULL) {
        // input is a float
        params->promotion_ratio = strtof(value, &end);
        params->batch_size = (uint64_t)(strtof(value, &end) * cache->cache_size);
      }
      else {
        params->batch_size = (uint64_t)strtol(value, &end, 0);
      }
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }
    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", lpFIFO_batch_current_params(cache, params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s, example paramters %s\n",
            cache->cache_name, key, lpFIFO_batch_current_params(cache, params));
      exit(1);
    }
  }
  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
