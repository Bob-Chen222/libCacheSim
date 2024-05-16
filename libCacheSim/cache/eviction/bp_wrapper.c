//
//  Batch-reinsertion modified based on Clock
//  Does not promote at eviction time, but periodically based on 
//  the provided constant param batch-size
//
//  bp_wrapper.c
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
  uint64_t queue_size;
  cache_obj_t **buffers; //buffers for each thread
  uint64_t num_thread;
} bp_wrapper_params_t;

static const char *DEFAULT_PARAMS = "batch-size=32,queue-size=64";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void bp_wrapper_parse_params(cache_t *cache,
                               const char *cache_specific_params);
static void bp_wrapper_free(cache_t *cache);
static bool bp_wrapper_get(cache_t *cache, const request_t *req);
static cache_obj_t *bp_wrapper_find(cache_t *cache, const request_t *req,
                               const bool update_cache);
static cache_obj_t *bp_wrapper_insert(cache_t *cache, const request_t *req);
static cache_obj_t *bp_wrapper_to_evict(cache_t *cache, const request_t *req);
static void bp_wrapper_evict(cache_t *cache, const request_t *req);
static bool bp_wrapper_remove(cache_t *cache, const obj_id_t obj_id);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

/**
 * @brief initialize a bp_wrapper cache
 *
 * @param ccache_params some common cache paramexters
 * @param cache_specific_params bp_wrapper specific parameters as a string
 */
cache_t *bp_wrapper_init(const common_cache_params_t ccache_params,
                    const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("bp_wrapper", ccache_params, cache_specific_params);
  cache->cache_init = bp_wrapper_init;
  cache->cache_free = bp_wrapper_free;
  cache->get = bp_wrapper_get;
  cache->find = bp_wrapper_find;
  cache->insert = bp_wrapper_insert;
  cache->evict = bp_wrapper_evict;
  cache->remove = bp_wrapper_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = bp_wrapper_to_evict;
  cache->obj_md_size = 0;
  pthread_spin_init(&cache->lock, 0);

#ifdef USE_BELADY
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "bp_wrapper_Belady");
#endif

  cache->eviction_params = malloc(sizeof(bp_wrapper_params_t));
  memset(cache->eviction_params, 0, sizeof(bp_wrapper_params_t));
  bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;

  params->batch_size = 32;
  params->num_thread = ccache_params.num_thread;

  bp_wrapper_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    bp_wrapper_parse_params(cache, cache_specific_params);
  }
  common_cache_params_t ccache_params_local = ccache_params;
  if (params->num_thread == 0){
    ERROR("num-thread must be set\n");
  }

  // allocate buffer
  // params->buffers = malloc(sizeof(cache_obj_t **) * params->num_thread);

  // for (int i = 0; i < params->num_thread; i++) {
  //   params->buffers[i] = malloc(sizeof(cache_obj_t *) * params->queue_size);
  // }


  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "bp_wrapper-%ld",
             params->batch_size);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void bp_wrapper_free(cache_t *cache) {
  bp_wrapper_params_t *params = (bp_wrapper_params_t *)(cache->eviction_params);
  // free(params->buffer);
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
static bool bp_wrapper_get(cache_t *cache, const request_t *req) {
  bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;

  // pthread_spin_lock(&cache->lock);

  static __thread int pos = 0;
  static __thread cache_obj_t **buff = NULL;
  static __thread int queue_size = 0;
  static __thread int batch_size = 0;
  static __thread bool allocated = false;
  static __thread bool trylock_outcome = false;

  // first allocate buffer
  if (!allocated) {
    buff = malloc(sizeof(cache_obj_t *) * params->queue_size);
    // allocate buffer
    for (int i = 0; i < params->queue_size; i++) {
      buff[i] = NULL;
    }
    queue_size = params->queue_size;
    batch_size = params->batch_size;
    allocated = true;
  }

  // pthread_spin_lock(&cache->lock);
  cache_obj_t *obj = cache_find_base(cache, req, true);
  // if (false){
  if (obj != NULL) { // hit
    // pthread_spin_lock(&cache->lock);
    assert(obj->queue.prev || obj -> queue.next);
    buff[pos] = obj;
    pos += 1;
    
    if (pos >= batch_size) {
      //pseudocode
      // trylock_outcome = pthread_spin_trylock(&cache->lock);
    }else{
      // there are still fewer objects than batch-size so we just stop here
      // pthread_spin_unlock(&cache->lock);
      return true;
    }
    //0 succeeds 1 fails
    if (trylock_outcome){
      if (pos < queue_size){
        DEBUG_ASSERT(false);
        // pthread_spin_unlock(&cache->lock);
        return true;
      }else{
        // pthread_spin_lock(&cache->lock);
      }
    }

    // do the prefetching for all objects currently in buffer
    for (int i = 0; i < pos; i++) {
      __builtin_prefetch(&buff[i]);
    }
    for (int i = 0; i < pos; i++) {
      cache_obj_t *obj = buff[i];
      // promote it to the head of the queue
      // for now I assume that it is in the cache
      DEBUG_ASSERT(!is_loop_bp(params->q_head, params->q_tail));
      if (hashtable_find_obj(cache->hashtable, obj)){
        DEBUG_ASSERT(hashtable_find_obj(cache->hashtable, obj));
        if (!contains_object(params->q_head, obj)){
          printf("thread %d not found: %d\n", req->thread_id, obj->obj_id);
        }
        DEBUG_ASSERT(contains_object(params->q_head, obj));
        // printf("moved to head1: %d\n", obj->obj_id);
        move_obj_to_head(&params->q_head, &params->q_tail, obj);
        // printf("211\n");
        // verify_hashtable(cache->hashtable, params->q_head);
        DEBUG_ASSERT(params->q_head == obj);
      }
    }

    // unlock!!
    // pthread_spin_unlock(&cache->lock);
    pos = 0;
    trylock_outcome = false;
    return true;
  }else{
    // do the prefetching for all objects currently in buffer
    for (int i = 0; i < pos; i++) {
      __builtin_prefetch(&buff[i]);
    }
    // lock
    // pthread_spin_lock(&cache->lock);
    for (int i = 0; i < pos; i++) {
      cache_obj_t *obj = buff[i];
      // promote it to the head of the queue
      // for now I assume that it will always be in the cache
      DEBUG_ASSERT(!is_loop_bp(params->q_head, params->q_tail));
      // DEBUG_ASSERT(hashtable_find(cache->hashtable, obj));
      if (hashtable_find_obj(cache->hashtable, obj)){
        DEBUG_ASSERT(hashtable_find_obj(cache->hashtable, obj));
        if (!contains_object(params->q_head, obj)){
          for (int j = 0; j < 10000; j++) {
            printf("thread %d not found: %d\n", req->thread_id, obj->obj_id);
          }
          // printf("not found: %d\n", obj->obj_id);
        }
        // printf("moved to head2: %d\n", obj->obj_id);
        // DEBUG_ASSERT(contains_object(params->q_head, obj));
        move_obj_to_head(&params->q_head, &params->q_tail, obj);
        // printf("239\n");
        // verify_hashtable(cache->hashtable, params->q_head);
        DEBUG_ASSERT(params->q_head == obj);
      }
    }
    // since this is a miss, we evict one object and add one object to the cache
    //evict
    if (cache->get_occupied_byte(cache) + req->obj_size +
               cache->obj_md_size >
           cache->cache_size) {
      DEBUG_ASSERT(cache->obj_md_size == 0);
      cache_obj_t *obj_to_evict = params->q_tail;
      DEBUG_ASSERT(hashtable_find_obj(cache->hashtable, obj_to_evict));
      bp_wrapper_evict(cache, req);
      printf("thread %d evicted %d\n", req->thread_id, obj_to_evict->obj_id);
      // verify_hashtable(cache->hashtable, params->q_head);
      DEBUG_ASSERT(!hashtable_find_obj(cache->hashtable, obj_to_evict));
      DEBUG_ASSERT(!contains_object(params->q_head, obj_to_evict));
    }

    // it is possible that the insertion failed as it is possible that both threads go to the miss scheme
    bp_wrapper_insert(cache, req);
    // printf("inserted %d\n", req->obj_id);
    // verify_hashtable(cache->hashtable, params->q_head);


    // unlock
    pos = 0;
    // pthread_spin_unlock(&cache->lock);
    return false;
  }
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
static cache_obj_t *bp_wrapper_find(cache_t *cache, const request_t *req,
                               const bool update_cache) {
  const bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;

  //I believe that we basially do not need to anything
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
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
static cache_obj_t *bp_wrapper_insert(cache_t *cache, const request_t *req) {
  cache_obj_t *obj = cache_insert_base(cache, req);
  if (obj != NULL) {
    bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;
    prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
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
static cache_obj_t *bp_wrapper_to_evict(cache_t *cache, const request_t *req) {
  DEBUG_ASSERT(false); //should never be called
  return NULL;
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
static void bp_wrapper_evict(cache_t *cache, const request_t *req) {
  bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;
  cache_obj_t *obj_to_evict = params->q_tail;
  DEBUG_ASSERT(params->q_tail != NULL);
  params->q_tail = params->q_tail->queue.prev;
  if (likely(params->q_tail != NULL)) {
    params->q_tail->queue.next = NULL;
  } else {
    /* cache->n_obj has not been updated */
    DEBUG_ASSERT(cache->n_obj == 1);
    params->q_head = NULL;
  }
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
static void bp_wrapper_remove_obj(cache_t *cache, cache_obj_t *obj) {
  DEBUG_ASSERT(false); //should never be called
  bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;
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
static bool bp_wrapper_remove(cache_t *cache, const obj_id_t obj_id) {
  DEBUG_ASSERT(false); //should never be called
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  bp_wrapper_remove_obj(cache, obj);

  return true;
}

// ***********************************************************************
// ****                                                               ****
// ****                  parameter set up functions                   ****
// ****                                                               ****
// ***********************************************************************
static const char *bp_wrapper_current_params(cache_t *cache,
                                        bp_wrapper_params_t *params) {
  static __thread char params_str[128];
  int n =
      snprintf(params_str, 128, "batch-size=%lu\n", params->batch_size);

  return params_str;
}

static void bp_wrapper_parse_params(cache_t *cache,
                               const char *cache_specific_params) {
  printf("cache_specific: %s\n", cache_specific_params);
  bp_wrapper_params_t *params = (bp_wrapper_params_t *)cache->eviction_params;
  char *params_str = strdup(cache_specific_params);
  char *old_params_str = params_str;
  char *end = NULL;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    printf("params_str: %s\n", params_str);
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }

    if (strcasecmp(key, "batch-size") == 0) {
      if (strchr(value, '.') != NULL) {
        // input is a float
        // cannot be float
        DEBUG_ASSERT("batch-size cannot be a float");
      }
      else {
        params->batch_size = (uint64_t)strtol(value, &end, 0);
      }
    } else if (strcasecmp(key, "queue-size") == 0) {
        params->queue_size = (uint64_t)strtol(value, &end, 0);
    }
    else if (strcasecmp(key, "print") == 0) {
      exit(0);
    }
    else {
      ERROR("%s does not have parameter %s, example paramters %s\n",
            cache->cache_name, key, bp_wrapper_current_params(cache, params));
      exit(1);
    }

  }
  free(old_params_str);

}

#ifdef __cplusplus
}
#endif
