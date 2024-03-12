//
//  sharded clock implementation
//  Note: supports different obj sizes, but may be inefficient
//        because the hashing does not consider size as weight
//
//  lpFIFO_shards.c
//  libCacheSim
//
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../dataStructure/hash/hash.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lpFIFO_shards_params {
  cache_t **shards;
  int n_shards;
  // a temporary request used to move object between shards
  request_t *req_local;
} lpFIFO_shards_params_t;

// #define LAZY_PROMOTION


static const char *DEFAULT_CACHE_PARAMS = "n-shards=4";

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************

static void lpFIFO_shards_parse_params(cache_t *cache,
                                const char *cache_specific_params);
static void lpFIFO_shards_free(cache_t *cache);
static bool lpFIFO_shards_get(cache_t *cache, const request_t *req);
static cache_obj_t *lpFIFO_shards_find(cache_t *cache, const request_t *req,
                                const bool update_cache);
static cache_obj_t *lpFIFO_shards_insert(cache_t *cache, const request_t *req);
static cache_obj_t *lpFIFO_shards_to_evict(cache_t *cache, const request_t *req);
static void lpFIFO_shards_evict(cache_t *cache, const request_t *req);
static bool lpFIFO_shards_remove(cache_t *cache, const obj_id_t obj_id);

/* lpFIFO_shards cannot an object larger than shard size */
static inline bool lpFIFO_shards_can_insert(cache_t *cache, const request_t *req);
static inline int64_t lpFIFO_shards_get_occupied_byte(const cache_t *cache);
static inline int64_t lpFIFO_shards_get_n_obj(const cache_t *cache);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ****                       init, free, get                         ****
// ***********************************************************************
/**
 * @brief initialize the cache
 *
 * @param ccache_params some common cache parameters
 * @param cache_specific_params cache specific parameters, see parse_params
 * function or use -e "print" with the cachesim binary
 */
cache_t *lpFIFO_shards_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params) {
  cache_t *cache =
      cache_struct_init("lpFIFO_shards", ccache_params, cache_specific_params);
  cache->cache_init = lpFIFO_shards_init;
  cache->cache_free = lpFIFO_shards_free;
  cache->get = lpFIFO_shards_get;
  cache->find = lpFIFO_shards_find;
  cache->insert = lpFIFO_shards_insert;
  cache->evict = lpFIFO_shards_evict;
  cache->remove = lpFIFO_shards_remove;
  cache->to_evict = lpFIFO_shards_to_evict;
  cache->can_insert = lpFIFO_shards_can_insert;
  cache->get_occupied_byte = lpFIFO_shards_get_occupied_byte;
  cache->get_n_obj = lpFIFO_shards_get_n_obj;

  if (ccache_params.consider_obj_metadata) {
    cache->obj_md_size = 8 * 2;
  } else {
    cache->obj_md_size = 0;
  }

  cache->eviction_params = (lpFIFO_shards_params_t *)malloc(sizeof(lpFIFO_shards_params_t));
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);

  lpFIFO_shards_parse_params(cache, DEFAULT_CACHE_PARAMS);
  if (cache_specific_params != NULL) {
    lpFIFO_shards_parse_params(cache, cache_specific_params);
  }

  if (params->n_shards > ccache_params.cache_size) {
    params->n_shards = ccache_params.cache_size;
  }


  params->shards = (cache_t **)malloc(sizeof(cache_t *) * params->n_shards);

  common_cache_params_t ccache_params_local = ccache_params;
  ccache_params_local.cache_size /= params->n_shards;
  if (ccache_params_local.cache_size < 1){
    ccache_params_local.cache_size = 1;
  }
  // ccache_params_local.hashpower = MIN(16, ccache_params_local.hashpower - 4);
  printf("cache size: %ld\n", ccache_params_local.cache_size);
  for (int i = 0; i < params->n_shards; i++) {
    params->shards[i] = FIFO_init(ccache_params_local, NULL);
  }
  params->req_local = new_request();
  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "lpFIFO_shard-%d",
             params->n_shards);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void lpFIFO_shards_free(cache_t *cache) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);
  free_request(params->req_local);
  for (int i = 0; i < params->n_shards; i++)
    params->shards[i]->cache_free(params->shards[i]);
  free(params->shards);
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
static bool lpFIFO_shards_get(cache_t *cache, const request_t *req) {
  /* because this field cannot be updated in time since shards are
   * updated, so we should not use this field */
  DEBUG_ASSERT(cache->occupied_byte == 0);

  bool ck = cache_get_base(cache, req);

  return ck;
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
static cache_obj_t *lpFIFO_shards_find(cache_t *cache, const request_t *req,
                                const bool update_cache) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);

  cache_obj_t *obj = NULL;

  // Check shard of hashed id for quick lookup
  uint64_t hashed_id = get_hash_value_int_64(&(req->obj_id)) % (params->n_shards);
  cache_t *shard = params->shards[hashed_id];
  obj = shard->find(shard, req, false);

  // If found matching obj without hash collision
  if (obj != NULL && obj->obj_id == req->obj_id) {
    if (!update_cache) {
      return obj;
    } 
    else {
      shard->find(shard, req, update_cache);
    }
    return obj;
  }

  // Since collision is rare (P ~= P_col + 1/n_shards)
  // When collision occurs, ignore and pretend obj is not in cache
  // Guarantees no duplicates in each shard (since for duplicates 
  // id and hashed_id must both match)
  return NULL;

  // Not found, scan all shards to find
  for (int i = 0; i < params->n_shards; i++) {
    shard = params->shards[i];
    obj = shard->find(shard, req, false);
    bool cache_hit = obj != NULL;

    if (obj == NULL) {
      continue;
    }

    if (!update_cache) {
      return obj;
    }

    if (cache_hit) {
      shard->find(shard, req, update_cache);
    }

    return obj;
  }

  return NULL;
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
static cache_obj_t *lpFIFO_shards_insert(cache_t *cache, const request_t *req) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);

  // // this is used by eviction age tracking
  // params->shards[0]->n_req = cache->n_req;

  // Choose shard based on hashed obj_id
  uint64_t hashed_id = get_hash_value_int_64(&(req->obj_id)) % (params->n_shards);
  cache_t *shard = params->shards[hashed_id];
  while (shard->get_occupied_byte(shard) + req->obj_size + cache->obj_md_size >
         shard->cache_size) {
    // lpFIFO_shards_evict(cache, req);
    shard->evict(shard, req);
  }
  return shard->insert(shard, req);
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
static cache_obj_t *lpFIFO_shards_to_evict(cache_t *cache, const request_t *req) {
  // lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);
  // uint64_t hashed_id = get_hash_value_int_64(&(req->obj_id)) % (params->n_shards);
  // cache_t *shard = params->shards[hashed_id];
  // return shard->to_evict(shard, req);
  assert(false);
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
static void lpFIFO_shards_evict(cache_t *cache, const request_t *req) {
  // Deprecate, do NOT use! eviction dependent on find/insert
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);
  uint64_t hashed_id = get_hash_value_int_64(&(req->obj_id)) % (params->n_shards);
  cache_t *shard = params->shards[hashed_id];
  shard->evict(shard, req);
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
static bool lpFIFO_shards_remove(cache_t *cache, const obj_id_t obj_id) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)(cache->eviction_params);
  for (int i = 0; i < params->n_shards; i++) {
    cache_t *shard = params->shards[i];
    if (shard->remove(shard, obj_id)) {
      return true;
    }
  }
  return false;
}

/* lpFIFO_shards cannot an object larger than shard size */
static inline bool lpFIFO_shards_can_insert(cache_t *cache, const request_t *req) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)cache->eviction_params;
  bool can_insert = cache_can_insert_default(cache, req);
  return can_insert &&
         (req->obj_size + cache->obj_md_size <= params->shards[0]->cache_size);
}

static inline int64_t lpFIFO_shards_get_occupied_byte(const cache_t *cache) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)cache->eviction_params;
  int64_t occupied_byte = 0;
  for (int i = 0; i < params->n_shards; i++) {
    occupied_byte += params->shards[i]->get_occupied_byte(params->shards[i]);
  }
  return occupied_byte;
}

static inline int64_t lpFIFO_shards_get_n_obj(const cache_t *cache) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)cache->eviction_params;
  int64_t n_obj = 0;
  for (int i = 0; i < params->n_shards; i++) {
    n_obj += params->shards[i]->get_n_obj(params->shards[i]);
  }
  return n_obj;
}

// ***********************************************************************
// ****                                                               ****
// ****                parameter set up functions                     ****
// ****                                                               ****
// ***********************************************************************
static const char *lpFIFO_shards_current_params(lpFIFO_shards_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "n-shards=%d\n", params->n_shards);
  return params_str;
}

static void lpFIFO_shards_parse_params(cache_t *cache,
                                const char *cache_specific_params) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)cache->eviction_params;
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

    if (strcasecmp(key, "n-shards") == 0) {
      params->n_shards = (int)strtol(value, &end, 0);
      if (strlen(end) > 2) {
        ERROR("param parsing error, find string \"%s\" after number\n", end);
      }

    } else if (strcasecmp(key, "print") == 0) {
      printf("current parameters: %s\n", lpFIFO_shards_current_params(params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s\n", cache->cache_name, key);
      exit(1);
    }
  }
  free(old_params_str);
}

// ***********************************************************************
// ****                                                               ****
// ****              cache internal functions                         ****
// ****                                                               ****
// ***********************************************************************


// ***********************************************************************
// ****                                                               ****
// ****                       debug functions                         ****
// ****                                                               ****
// ***********************************************************************
static void lpFIFO_shards_print_cache(cache_t *cache) {
  lpFIFO_shards_params_t *params = (lpFIFO_shards_params_t *)cache->eviction_params;
  for (int i = params->n_shards - 1; i >= 0; i--) {
    cache_obj_t *obj =
        ((Clock_params_t *)params->shards[i]->eviction_params)->q_head;
    while (obj) {
      printf("%ld(%u)->", obj->obj_id, obj->obj_size);
      obj = obj->queue.next;
    }
    printf(" | ");
  }
  printf("\n");
}

#ifdef __cplusplus
extern "C"
}
#endif
