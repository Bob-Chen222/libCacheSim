//
//  a FH module that supports different obj size
//
//
//  FH.c
//  libCacheSim
//
//  Created by Juncheng on 12/4/18.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

// TODO: construction at the background
// TODO: during construction, serving using regular LRU but do not promote
// TODO: when construction is ready, set the flag and all the requests go to the frozen mode
// TODO: when deciding the split point, also allow new objects to be insert at the front, freeze at approximate point
// and then start the construciton using one extra thread

// free of hashtable can be done during the construction phase instead of the deconstruction phase
// merging two linkedlist can be done without doing it in background???
// TODO: add one extra hashnext pointer to the hashtable so that they can achieve the shared object status
// TODO: use shared objects instead of creating a copy

#ifdef __cplusplus
extern "C" {
#endif

// #define USE_BELADY
typedef struct {
  cache_obj_t *f_head;
  cache_obj_t *f_tail; //current design is that the next element of f_tail is d_head
  cache_obj_t *d_head;
  cache_obj_t *d_tail;
  cache_obj_t *q_head;
  cache_obj_t *q_tail;

  float miss_ratio_diff; //the target miss ratio difference
  hashtable_t *hash_table_f; //only FC needs a hash table
  float split_point; //from left to right
  int split_obj; //the number of objects in the frozen list
  uint64_t frozen_cache_miss;
  uint64_t frozen_cache_access;

  float    regular_miss_ratio; //the miss ratio from previous base LRU cache period
  uint64_t regular_cache_miss;
  uint64_t regular_cache_access;
  
  
  bool constucting;
  bool called_construction; //this is to ensure that no more than one construction is called at a time
  bool called_deconstruction;
  bool is_frozen;

  int num_extra_thread;

} FH_params_t;

static const char *DEFAULT_PARAMS = "split_point=0.2,miss_ratio_diff=0.05";
// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void FH_parse_params(cache_t *cache,
                               const char *cache_specific_params);
static void FH_free(cache_t *cache);
static bool FH_get(cache_t *cache, const request_t *req);
static bool FH_Frozen_get(cache_t *cache, const request_t *req, FH_params_t *params);
static bool FH_Regular_get(cache_t *cache, const request_t *req, FH_params_t *params);
static void deconstruction(cache_t *cache, FH_params_t *params);
static void construction(void *cache);
static void FH_remove_obj(cache_t *cache, cache_obj_t *obj);
static bool FH_lru_get(cache_t *cache, const request_t *req, FH_params_t *params, const bool from_regular);
static cache_obj_t *FH_lru_find(cache_t *cache, const request_t *req,
                             const bool from_regular);
static cache_obj_t *FH_lru_insert(cache_t *cache, const request_t *req);
static cache_obj_t *FH_to_evict(cache_t *cache, const request_t *req);
static void FH_lru_evict(cache_t *cache, const request_t *req);
static bool FH_remove(cache_t *cache, const obj_id_t obj_id);
static void FH_print_cache(const cache_t *cache);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************
/**
 * @brief initialize a FH cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params FH specific parameters, should be NULL
 */
cache_t *FH_init(const common_cache_params_t ccache_params,
                  const char *cache_specific_params) {
  cache_t *cache = cache_struct_init("FH", ccache_params, cache_specific_params);
  cache->cache_init = FH_init;
  cache->cache_free = FH_free;
  cache->get = FH_get;
  cache->find = FH_lru_find;
  cache->insert = FH_lru_insert;
  cache->evict = FH_lru_evict;
  cache->remove = FH_remove;
  cache->can_insert = cache_can_insert_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->to_evict = FH_to_evict;
  cache->obj_md_size = 0;
  pthread_spin_init(&cache->lock, 0);

  cache->eviction_params = malloc(sizeof(FH_params_t));
  memset(cache->eviction_params, 0, sizeof(FH_params_t));
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  params->f_head = NULL;
  params->f_tail = NULL;
  params->d_head = NULL;
  params->d_tail = NULL;
  params->q_head = NULL;
  params->q_tail = NULL;
  params->hash_table_f = NULL;
  params->constucting = false;
  params->is_frozen = false;
  params->called_deconstruction = false;
  params->called_construction = false;
  params->num_extra_thread = 0;
  FH_parse_params(cache, DEFAULT_PARAMS);
  if (cache_specific_params != NULL) {
    FH_parse_params(cache, cache_specific_params);
  }
  params->split_obj = cache->cache_size * params->split_point;
  if (params->split_obj == 0){
    INFO("split_object is 0 meaning it will be LRU\n");
  }
  return cache;
}

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
static cache_obj_t* FH_lru_find(cache_t *cache, const request_t *req,
                             const bool from_regular) {
  pthread_spin_lock(&cache->lock);                           
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  cache_obj_t *cache_obj = cache_find_base(cache, req, true);

  if (cache_obj && likely(true)) {
    if (!__atomic_load_n(&params->constucting, __ATOMIC_RELAXED)){
      // only promote object in qlist
      // DEBUG
      if (params->is_frozen && from_regular){
        pthread_spin_unlock(&cache->lock);
        return cache_obj;
      }
      move_obj_to_head(&params->q_head, &params->q_tail, cache_obj); //bug source
    }
  } 
  pthread_spin_unlock(&cache->lock);
  return cache_obj;
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 */
static void FH_lru_evict(cache_t *cache, const request_t *req) {
  pthread_spin_lock(&cache->lock);
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  if (__atomic_load_n(&params->constucting, __ATOMIC_RELAXED)){
    pthread_spin_unlock(&cache->lock);
    return;
  }
  cache_obj_t *obj_to_evict = NULL;
  obj_to_evict = params->q_tail;
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
  pthread_spin_unlock(&cache->lock);
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
static cache_obj_t *FH_lru_insert(cache_t *cache, const request_t *req) {
  pthread_spin_lock(&cache->lock);
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  if (__atomic_load_n(&params->constucting, __ATOMIC_RELAXED)){
    pthread_spin_unlock(&cache->lock);
    return NULL;
  }
  cache_obj_t *obj = cache_insert_base(cache, req);
  if (obj != NULL){
    // TODO: might need to change later that to support insert during construction
    prepend_obj_to_head(&params->q_head, &params->q_tail, obj);
  }
  pthread_spin_unlock(&cache->lock);
  return obj;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void FH_free(cache_t *cache) { 
  FH_params_t* params = (FH_params_t*)cache->eviction_params;
  // looping
  // while (params->num_extra_thread){
  // }
  // destroy the lock
  if (params->hash_table_f != NULL){
    free_hashtable(params->hash_table_f);
  }
  free(cache->eviction_params);
  cache_struct_free(cache);
}

static bool FH_lru_get(cache_t *cache, const request_t *req, FH_params_t *params, const bool from_regular){
  cache_obj_t *obj = FH_lru_find(cache, req, true);
  bool hit = (obj != NULL);
  if (hit){
    return hit;
  }

  // printf("this should happen I believe\n");
  if (!cache->can_insert(cache, req)) {
    VVERBOSE("req %ld, obj %ld --- cache miss cannot insert\n", cache->n_req,
             req->obj_id);
  } else {
    if (cache->get_occupied_byte(cache) + req->obj_size +
               cache->obj_md_size >
           cache->cache_size) {
      FH_lru_evict(cache, req);
    }
    DEBUG_ASSERT(req->obj_id != 0);
    FH_lru_insert(cache, req);
  }

  return false;
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
static bool FH_get(cache_t *cache, const request_t *req) {
  // we just first do very regular LRU cache
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  // other threads need to check whether the cache is currently in construction
  if (__atomic_load_n(&params->is_frozen, __ATOMIC_RELAXED)){
    // do the FH operations including possible deconstructions
    return FH_Frozen_get(cache, req, params);
  }else{
    // do the regular cache operations including possible constructions
    return FH_Regular_get(cache, req, params);
  }
  return false;
}

static bool FH_Frozen_get(cache_t *cache, const request_t *req, FH_params_t *params){
  // we first check whether the frozen hashtable has the requested entry
  // printf("frozen id: %d\n", req->obj_id);
  
  params->frozen_cache_access++;
  // this is read-only so we in fact do not add any lock on this hashtable
  //TODO: create a new function for hashtable that does not need a lock
  cache_obj_t *obj = hashtable_f_find_obj_id(params->hash_table_f, req->obj_id);
  if (obj != NULL){
    return true;
  }else{
    // do regular LRU cache operations and change the related statistics
    bool result = FH_lru_get(cache, req, params, false);
    if (!result){
      params->frozen_cache_miss++;
    }else{
      return true;
    }
  }

  // check whether we need to reconstruct
  // TODO: I believe use rw lock in this case is the best
  float cur_miss_ratio = ((float)params->frozen_cache_miss / (float)params->frozen_cache_access);
  if (cur_miss_ratio - params->regular_miss_ratio > params->miss_ratio_diff && params->frozen_cache_access > cache->cache_size){
    // we need to reconstruct
    // TODO: check deconstruction
    bool TRUE = false;
    bool FALSE = true;
    if (__atomic_compare_exchange(&params->called_deconstruction, &TRUE, &FALSE, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)){
      INFO("start deconstructing\n");
      params->called_deconstruction = true;
      deconstruction(cache, params);
    }
  }
  return false;
}

static bool FH_Regular_get(cache_t *cache, const request_t *req, FH_params_t *params){
  // we just first do very regular LRU cache
  bool res = false;
  params->regular_cache_access++;
  res = FH_lru_get(cache, req, params, true);
  if (!res && params->regular_cache_access > cache -> cache_size){
    __atomic_fetch_add(&params->regular_cache_miss, 1, __ATOMIC_RELAXED);
  }

  // check whether it is time for reconstruction
  // 1. the cache should be full
  // 2. the cache should already wait for 2 * cache_size accesses
  // pthread_rwlock_unlock(&params->constructing);
  if ((params->regular_cache_access >= 2 * cache->cache_size) && (cache->n_obj >= cache->cache_size)){
    // do compare and set and if it is true then go on
    bool TRUE = false;
    bool FALSE = true;
    if (__atomic_compare_exchange(&params->called_construction, &TRUE, &FALSE, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)){
      INFO("start constructing\n");
      params->called_construction = true;
      pthread_t construct_thread;
      pthread_create(&construct_thread, NULL, (void *)construction, (void *)cache);
      pthread_detach(construct_thread);
    }
  }

  return res;
}

static void deconstruction(cache_t *cache, FH_params_t *params){
  // merge the two lists
  if (params->f_head != NULL){
    params->f_tail->queue.next = params->q_head;
    params->q_head->queue.prev = params->f_tail;
    params->q_head = params->f_head;
  }
  // destroy the hash table, but cannot delete the objects
  params->regular_cache_access = 0;
  params->regular_cache_miss = 0;
  params->regular_miss_ratio = 0;
  params->f_head = NULL;

  params->is_frozen = false;
  // __atomic_store(&params->constucting, &FALSE, __ATOMIC_RELAXED);
  params->called_construction = false;
}

static void construction(void* c){
  cache_t *cache = (cache_t *)c;
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  atomic_fetch_add(&params->num_extra_thread, 1);
  params->regular_miss_ratio = ((float)params->regular_cache_miss / (float)(params->regular_cache_access - cache->cache_size));
  DEBUG_ASSERT(params->regular_miss_ratio >= 0);
  params->regular_cache_access = 0;
  params->regular_cache_miss = 0;
  // // destroy any previous hashtable
  if (params->hash_table_f != NULL){
    free_chained_hashtable_f_v2(params->hash_table_f);
    my_free(sizeof(cache_obj_t *) * hashsize(params->hash_table_f->hashpower),
          params->hash_table_f->ptr_table);
    my_free(sizeof(hashtable_t), params->hash_table_f);
  }
  params->hash_table_f = create_hashtable(10);
  // // split the list
  cache_obj_t *cur = params->q_head;
  int count = 0;
  
  int miss_cur = params->regular_cache_miss; //keep track of current misses so that we know how many newly inserted is in the hash
  // printf("miss_cur is %d\n", miss_cur);
  // printf("regular_cache_miss is %lu\n", params->regular_cache_miss);
  // printf("split_obj is %d\n", params->split_obj);
  while (count + params -> regular_cache_miss - miss_cur < params->split_obj){
    DEBUG_ASSERT(cur != NULL);
    cur = cur->queue.next;
    count++;
  }
  // printf("final count is %d\n", count);

  bool TRUE = true;
  bool FALSE = false;
  __atomic_store(&params->constucting, &TRUE, __ATOMIC_RELAXED);

  // at this point we know the split point but still need somehow a atomic system to make sure that it will not lead to racing
  // WARNING: my current decision is that I will use q_head as new FC head and hope that in this way it will not lead to racing
  // TODO: need to add extra hash pointer to support shared objects between two hashtables

  // after decided which point, we serve the request but do not promote the object
  for (cache_obj_t* tmp = params->q_head; tmp != cur; tmp = tmp->queue.next){
    DEBUG_ASSERT(tmp != NULL);
    hashtable_insert_obj(params->hash_table_f, tmp);
  }
  params->f_head = params->q_head;
  params->q_head = cur;
  params->f_tail = cur->queue.prev;
  params->f_tail->queue.next = NULL;
  params->q_head->queue.prev = NULL;

  

  //update stats after construction
  params->frozen_cache_access = 0;
  params->frozen_cache_miss = 0;

 

  __atomic_store(&params->is_frozen, &TRUE, __ATOMIC_RELAXED);
  // __atomic_store(&params->called_construction, &FALSE, __ATOMIC_RELAXED);
  __atomic_fetch_sub(&params->num_extra_thread, 1, __ATOMIC_RELAXED);
  DEBUG_ASSERT(params->f_head->queue.prev == NULL);
  params->called_deconstruction = false;
}




// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************

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
static cache_obj_t *FH_to_evict(cache_t *cache, const request_t *req) {
  FH_params_t *params = (FH_params_t *)cache->eviction_params;

  DEBUG_ASSERT(params->q_tail != NULL || cache->occupied_byte == 0);
  cache->to_evict_candidate_gen_vtime = cache->n_req;
  return params->q_tail;
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
static void FH_remove_obj(cache_t *cache, cache_obj_t *obj) {
  assert(obj != NULL);

  FH_params_t *params = (FH_params_t *)cache->eviction_params;

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
static bool FH_remove(cache_t *cache, const obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == NULL) {
    return false;
  }
  FH_params_t *params = (FH_params_t *)cache->eviction_params;

  remove_obj_from_list(&params->q_head, &params->q_tail, obj);
  cache_remove_obj_base(cache, obj, true);

  return true;
}

static void FH_print_cache(const cache_t *cache) {
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
  cache_obj_t *cur = params->q_head;
  // print from the most recent to the least recent
  if (cur == NULL) {
    printf("empty\n");
    return;
  }
  while (cur != NULL) {
    printf("%lu->", (unsigned long)cur->obj_id);
    cur = cur->queue.next;
  }
  printf("END\n");
}

static void FH_parse_params(cache_t *cache,
                               const char *cache_specific_params) {
  printf("cache_specific: %s\n", cache_specific_params);
  FH_params_t *params = (FH_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "split_point") == 0) {
      if (strchr(value, '.') != NULL) {
        // input is a float
        params->split_point = strtof(value, &end);
      }
      else {
        DEBUG_ASSERT("only support float\n");
      }
    } else if (strcasecmp(key, "miss_ratio_diff") == 0) {
      if (strchr(value, '.') != NULL) {
        // input is a float
        params->miss_ratio_diff = strtof(value, &end);
      }
      else {
        DEBUG_ASSERT("only support float\n");
      }
    } else if (strcasecmp(key, "print") == 0) {
      exit(0);
    }
    else {
      ERROR("error in FrozenHot Parameters!");
      exit(1);
    }

  }
  free(old_params_str);

}

#ifdef __cplusplus
}
#endif