//
// Created by Juncheng Yang on 6/9/20.
//

#ifndef libCacheSim_CHAINEDHASHTABLEV2_H
#define libCacheSim_CHAINEDHASHTABLEV2_H

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <stdbool.h>

#include "../../include/libCacheSim/cacheObj.h"
#include "../../include/libCacheSim/request.h"
#include "hashtableStruct.h"

hashtable_t *create_pchained_hashtable(const uint16_t hashpower_init);

cache_obj_t *pchained_hashtable_find_obj_id(const hashtable_t *hashtable,
                                              const obj_id_t obj_id);

cache_obj_t *pchained_hashtable_find(const hashtable_t *hashtable,
                                       const request_t *req);

cache_obj_t *pchained_hashtable_find_obj(const hashtable_t *hashtable,
                                           const cache_obj_t *obj_to_evict);

/* return an empty cache_obj_t */
cache_obj_t *pchained_hashtable_insert(hashtable_t *hashtable,
                                         const request_t *req);

cache_obj_t *pchained_hashtable_insert_obj(hashtable_t *hashtable,
                                             cache_obj_t *cache_obj);

bool pchained_hashtable_try_delete(hashtable_t *hashtable,
                                     cache_obj_t *cache_obj);

void pchained_hashtable_delete(hashtable_t *hashtable,
                                 cache_obj_t *cache_obj);

cache_obj_t *pchained_hashtable_rand_obj(const hashtable_t *hashtable);

void pchained_hashtable_foreach(hashtable_t *hashtable,
                                  hashtable_iter iter_func, void *user_data);

void free_pchained_hashtable(hashtable_t *hashtable);

void check_pchained_hashtable_integrity(const hashtable_t *hashtable);

void check_hashtable_integrity2_v2(const hashtable_t *hashtable,
                                   const cache_obj_t *head);

#ifdef __cplusplus
}
#endif

#endif  // libCacheSim_CHAINEDHASHTABLEV2_H
