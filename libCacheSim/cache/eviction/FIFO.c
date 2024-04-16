//
//  first in first out
//
//
//  FIFO.c
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

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void FIFO_parse_params(cache_t *cache,
                              const char *cache_specific_params);
static void FIFO_free(cache_t *cache);
static bool FIFO_get(cache_t *cache, const request_t *req);
static cache_obj_t *FIFO_find(cache_t *cache, const request_t *req,
                              const bool update_cache);
static cache_obj_t *FIFO_insert(cache_t *cache, const request_t *req);
static cache_obj_t *FIFO_to_evict(cache_t *cache, const request_t *req);
static void FIFO_evict(cache_t *cache, const request_t *req);
static bool FIFO_remove(cache_t *cache, const obj_id_t obj_id);
static void print_FIFO(cache_t *cache);
static bool doubly_linked_list_test(cache_t *cache);
static bool surjection_test(cache_t *cache);
static uint64_t test_and_set(uint64_t *dummy);
static void test_and_test_and_set(unsigned long* dummy);
static cache_obj_t* TT_evict_last_obj(cache_t *cache);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************

/**
 * @brief initialize a ARC cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params ARC specific parameters, should be NULL
 */
cache_t *FIFO_init(const common_cache_params_t ccache_params,
                   const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("FIFO", ccache_params, cache_specific_params);
  cache->cache_init = FIFO_init;
  cache->cache_free = FIFO_free;
  cache->get = FIFO_get;
  cache->find = FIFO_find;
  cache->insert = FIFO_insert;
  cache->evict = FIFO_evict;
  cache->remove = FIFO_remove;
  cache->to_evict = FIFO_to_evict;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->can_insert = cache_can_insert_default;
  cache->obj_md_size = 0;
  cache->warmup_complete = false;

  cache->eviction_params = malloc(sizeof(FIFO_params_t));
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->val_lock = UINT64_MAX;

  // add a dummy object to the queue so that tail will not change

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void FIFO_free(cache_t *cache) {
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
static bool FIFO_get(cache_t *cache, const request_t *req) {
  return cache_get_base(cache, req);
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
static cache_obj_t *FIFO_find(cache_t *cache, const request_t *req,
                               const bool update_cache) {
  return cache_find_base(cache, req, update_cache);
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
static cache_obj_t *FIFO_insert(cache_t *cache, const request_t *req) {
  // printf("no fifo insert\n");
  cache_obj_t *obj = cache_insert_base(cache, req);
  if (obj != NULL){
    FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
    if (!cache->warmup_complete){
      prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
      // DEBUG_ASSERT(doubly_linked_list_test(cache));
    }else{
      // printf("num objects: %lu\n", cache->n_obj);
      // pthread_spin_lock(&cache->lock);
      T_prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
      // DEBUG_ASSERT(surjection_test(cache));
      // pthread_spin_unlock(&cache->lock);

    }
  }

  return NULL; 
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
static cache_obj_t *FIFO_to_evict(cache_t *cache, const request_t *req) {
  printf("no fifo toevict\n");
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  return params->q_tail;
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
static void FIFO_evict(cache_t *cache, const request_t *req) {
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  DEBUG_ASSERT(params->q_tail != NULL);
  cache_obj_t *obj_to_evict = TT_evict_last_obj(cache);
  // DEBUG_ASSERT(obj_to_evict != NULL);

  // // TODO: change to true
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
static bool FIFO_remove(cache_t *cache, const obj_id_t obj_id) {
  printf("no fifo remove\n");
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }

  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;

  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
  cache_remove_obj_base(cache, obj, true);

  return true;
}

static void print_FIFO(cache_t *cache) {
  printf("no fifo print\n");
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  printf("FIFO queue: ");
  cache_obj_t *obj = params->q_head;
  while (obj != NULL) {
    printf("===%lu ", obj->obj_id);
    obj = obj->queue.next;
    DEBUG_ASSERT(obj != params->q_head);
  }

  printf("\n");
}

static bool surjection_test(cache_t *cache){
  // traverse the linked list and make sure that all the items are in the hashtable as well
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  cache_obj_t *p = params->q_head;
  while(p != NULL){
    if (hashtable_find_obj_id(cache->hashtable, p->obj_id) == NULL){
      printf("obj_id %lu not in hashtable\n", p->obj_id);
      return false;
    }
    p = p->queue.next;
  }
  return true;
}

static bool doubly_linked_list_test(cache_t *cache){
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  cache_obj_t *head = params->q_head;
  cache_obj_t *tail = params->q_tail;

  cache_obj_t *p = head;
  while(p -> queue.next != NULL){
    if (p->queue.next != NULL){
      if (p->queue.next->queue.prev != p){
        printf("next->prev not equal to p\n");
        return false;
      }
    }
    p = p->queue.next;
  }

  if (p != tail){
    printf("p != tail\n");
    return false;
  }

  if (p->queue.next != NULL){
    printf("p->next != NULL\n");
    return false;
  }

  return true;

}

static uint64_t test_and_set(uint64_t *dummy) {
    uint64_t expected = UINT64_MAX;
    // Atomic compare and exchange
    // If *lock_ptr == expected, then *lock_ptr is set to 1 and returns true (lock acquired)
    // If not, it does nothing and returns false
    return __atomic_compare_exchange_n(dummy, &expected, UINT64_MAX - 1, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}

static void test_and_test_and_set(unsigned long* dummy) {
    while (true) {
        // Busy wait if the lock is taken
        while (__atomic_load_n(dummy, __ATOMIC_RELAXED) == UINT64_MAX - 1){
            // Lock is busy, just wait
        }
        if (test_and_set(dummy)) {
            // Lock is acquired
            break;
        }
    }
}

static cache_obj_t* TT_evict_last_obj(cache_t *cache) {
  FIFO_params_t *params = (FIFO_params_t *)cache->eviction_params;
  // use test and test and set to lock the tail
  test_and_test_and_set(&params->val_lock);
  cache_obj_t *obj_to_evict = params->q_tail;
  // DEBUG_ASSERT(params->q_tail != NULL);
  params->q_tail = params->q_tail->queue.prev;
  if (likely(params->q_tail != NULL)) {
    params->q_tail->queue.next = NULL;
  } else {
    /* cache->n_obj has not been updated */
    DEBUG_ASSERT(cache->n_obj == 1);
    params->q_head = NULL;
  }
  // unlock the tail
  params->val_lock = UINT64_MAX;
  return obj_to_evict;
}

#ifdef __cplusplus
}
#endif
