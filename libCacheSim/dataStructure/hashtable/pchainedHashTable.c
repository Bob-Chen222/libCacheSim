//
// TODO
// This hash table stores pointers to cache_obj_t in the table, it uses
// one-level of indirection.
// draw a table
// |----------------|
// |     void*      | ----> cache_obj_t* ----> cache_obj_t* ----> NULL
// |----------------|
// |     void*      | ----> cache_obj_t*
// |----------------|
// |     void*      | ----> NULL
// |----------------|
// |     void*      | ----> cache_obj_t* ----> cache_obj_t* ----> nULL
// |----------------|
// |     void*      | ----> NULL
// |----------------|
// |     void*      | ----> NULL
// |----------------|
//
//

// lock principle
// 1. if outside write, then lock
// 2. if inside write, then no lock
// 3. if outside read, then r_lock

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <stdatomic.h>

#include "../../include/libCacheSim/logging.h"
#include "../../include/libCacheSim/macro.h"
#include "../../utils/include/mymath.h"
#include "../hash/hash.h"
#include "pchainedHashTable.h"

#define OBJ_EMPTY(cache_obj) ((cache_obj)->obj_size == 0)
#define NEXT_OBJ(cur_obj) (((cache_obj_t *)(cur_obj))->hash_next)

static void _pchained_hashtable_expand(hashtable_t *hashtable);
static void print_hashbucket_item_distribution(const hashtable_t *hashtable);

/************************ helper func ************************/
/**
 * get the last object in the hash bucket
 */
static inline cache_obj_t *_last_obj_in_bucket(const hashtable_t *hashtable,
                                               const uint64_t hv) {
  cache_obj_t *cur_obj_in_bucket = hashtable->ptr_table[hv];
  while (cur_obj_in_bucket->hash_next) {
    cur_obj_in_bucket = cur_obj_in_bucket->hash_next;
  }
  return cur_obj_in_bucket;
}

/* add an object to the hashtable */
// I believe that the thread first came will not interfere with any of the afterhand threads
// so the second thread can read safely
// we will use the original object as lock and it should be fine
static inline void add_to_bucket(hashtable_t *hashtable,
                                 cache_obj_t *cache_obj) {
  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);

  uint64_t mask = ~(1);

  cache_obj_t *dummy = hashtable->ptr_table[hv];
  cache_obj_t *old = (cache_obj_t*)((uint64_t)hashtable->ptr_table[hv] & mask);
  cache_obj_t *new = (cache_obj_t*)((uint64_t)hashtable->ptr_table[hv] | 1);

  while (!compare_and_set(&dummy, old, new)) {
  }

  // at this point the mask is 1
  if (old -> hash_next == NULL) {
    old -> hash_next = cache_obj;
    return;
  }
  cache_obj_t *head_ptr = old -> hash_next;

  cache_obj->hash_next = head_ptr;
  hashtable->ptr_table[hv]->hash_next = cache_obj;
  // this only is correct when we can safely switch at any point when completing the final step

#ifdef HASHTABLE_DEBUG
  cache_obj_t *curr_obj = cachec_obj->hash_next;
  while (curr_obj) {
    assert(curr_obj->obj_id != cache_obj->obj_id);
    curr_obj = curr_obj->hash_next;
  }
#endif
}

/* free object, called by other functions when iterating through the hashtable
 */
static inline void foreach_free_obj(cache_obj_t *cache_obj, void *user_data) {
  my_free(sizeof(cache_obj_t), cache_obj);
}

/************************ hashtable func ************************/
hashtable_t *create_pchained_hashtable(const uint16_t hashpower) {
  hashtable_t *hashtable = my_malloc(hashtable_t);
  memset(hashtable, 0, sizeof(hashtable_t));

  size_t size = sizeof(cache_obj_t *) * hashsize(hashtable->hashpower);
  hashtable->ptr_table = my_malloc_n(cache_obj_t *, hashsize(hashpower));
  if (hashtable->ptr_table == NULL) {
    ERROR("allcoate hash table %zu entry * %lu B = %ld MiB failed\n",
          sizeof(cache_obj_t *), (unsigned long)(hashsize(hashpower)),
          (long)(sizeof(cache_obj_t *) * hashsize(hashpower) / 1024 / 1024));
    exit(1);
  }
  memset(hashtable->ptr_table, 0, size);

#ifdef USE_HUGEPAGE
  madvise(hashtable->table, size, MADV_HUGEPAGE);
#endif
  hashtable->external_obj = false;
  hashtable->hashpower = hashpower;
  hashtable->n_obj = 0;
  // for every entry, add a dummy object
  for (uint64_t i = 0; i < hashsize(hashtable->hashpower); i++) {
    request_t* req = new_request();
    cache_obj_t* cache_obj = create_cache_obj_from_request(req);
    hashtable->ptr_table[i] = cache_obj;
  }
  return hashtable;
}

cache_obj_t *pchained_hashtable_find_obj_id(const hashtable_t *hashtable,
                                              const obj_id_t obj_id) {
  // add readlock
  // we will use the same lock
  uint64_t mask = ~(1); //todo: check!!
  cache_obj_t *cache_obj = NULL;
  uint64_t hv = get_hash_value_int_64(&obj_id);
  hv = hv & hashmask(hashtable->hashpower);

  cache_obj_t *dummy = hashtable->ptr_table[hv];
  cache_obj_t *old = (cache_obj_t*)((uint64_t)hashtable->ptr_table[hv] & mask);
  cache_obj_t *new = (cache_obj_t*)((uint64_t)hashtable->ptr_table[hv] | 1);

  while (!compare_and_set(&dummy, old, new)) {
  }

  cache_obj = hashtable->ptr_table[hv] -> hash_next;

  while (cache_obj) {
    if (cache_obj->obj_id == obj_id) {
      return cache_obj;
    }
    cache_obj = cache_obj->hash_next;
  }
  hashtable->ptr_table[hv] = (cache_obj_t*)((uint64_t)hashtable->ptr_table[hv] & mask);
  return cache_obj;
}

cache_obj_t *pchained_hashtable_find(const hashtable_t *hashtable,
                                       const request_t *req) {
  //add readwrite lock
  return pchained_hashtable_find_obj_id(hashtable, req->obj_id);
}

cache_obj_t *pchained_hashtable_find_obj(const hashtable_t *hashtable,
                                           const cache_obj_t *obj_to_find) {
  return pchained_hashtable_find_obj_id(hashtable, obj_to_find->obj_id);
}

/* the user needs to make sure the added object is not in the hash table */
cache_obj_t *pchained_hashtable_insert(hashtable_t *hashtable,
                                         const request_t *req) {
  cache_obj_t *new_cache_obj = create_cache_obj_from_request(req);
  add_to_bucket(hashtable, new_cache_obj);
  // use atomic add instead
  atomic_fetch_add(&hashtable->n_obj, 1);
  return new_cache_obj;
}

/* the user needs to make sure the added object is not in the hash table */
cache_obj_t *pchained_hashtable_insert_obj(hashtable_t *hashtable,
                                             cache_obj_t *cache_obj) {
  DEBUG_ASSERT(hashtable->external_obj);
  
  if (hashtable->n_obj > (uint64_t)(hashsize(hashtable->hashpower) *
                                    CHAINED_HASHTABLE_EXPAND_THRESHOLD))
    _chained_hashtable_expand_v2(hashtable);

  add_to_bucket(hashtable, cache_obj);
  hashtable->n_obj += 1;

  return cache_obj;
}

/* you need to free the extra_metadata before deleting from hash table */
void pchained_hashtable_delete(hashtable_t *hashtable,
                                 cache_obj_t *cache_obj) {
  // use atomic sub instead
  // first check whether the whole hashtable is in the process of expanding
  uint64_t mask = ~(1); //1111...0
  cache_obj_t *old = (cache_obj_t*)((uint64_t)hashtable->ptr_table & mask);
  cache_obj_t *new = (cache_obj_t*)((uint64_t)hashtable->ptr_table | 1);
  //at this moment the expand completes
  atomic_fetch_add(&hashtable->n_obj, -1);
  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);
  while (!compare_and_set(&hashtable->ptr_table[hv], old, new)) {
  }
  if ((old -> hash_next) == cache_obj) {
    old->hash_next = cache_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cache_obj);
    return;
  }

  static int max_chain_len = 16;
  int chain_len = 1;
  cache_obj_t *cur_obj = old -> hash_next;
  while (cur_obj != NULL && cur_obj->hash_next != cache_obj) {
    cur_obj = cur_obj->hash_next;
    chain_len += 1;
  }

  if (chain_len > max_chain_len) {
    max_chain_len = chain_len;
    WARN("hashtable remove %lu chain len %d, hashtable load %ld/%ld %lf\n",
         (unsigned long)cache_obj->obj_id, max_chain_len,
         (long)hashtable->n_obj, (long)hashsize(hashtable->hashpower),
         (double)hashtable->n_obj / hashsize(hashtable->hashpower));

    print_hashbucket_item_distribution(hashtable);
  }

  // the object to remove is not in the hash table
  DEBUG_ASSERT(cur_obj != NULL);
  cur_obj->hash_next = cache_obj->hash_next;
  // release the lock manually
  hashtable->ptr_table[hv] = old;
  if (!hashtable->external_obj) {
    free_cache_obj(cache_obj);
  }
}

// bTD: check again for this function see whehter it is being referenced in other places
bool pchained_hashtable_try_delete(hashtable_t *hashtable,
                                     cache_obj_t *cache_obj) {
  static int max_chain_len = 1;

  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);
  if (hashtable->ptr_table[hv] == cache_obj) {
    hashtable->ptr_table[hv] = cache_obj->hash_next;
    hashtable->n_obj -= 1;
    if (!hashtable->external_obj) free_cache_obj(cache_obj);
    return true;
  }

  int chain_len = 1;
  cache_obj_t *cur_obj = hashtable->ptr_table[hv];
  while (cur_obj != NULL && cur_obj->hash_next != cache_obj) {
    cur_obj = cur_obj->hash_next;
    chain_len += 1;
  }

  if (chain_len > 16 && chain_len > max_chain_len) {
    max_chain_len = chain_len;
    //    WARN("hashtable remove %ld, hv %lu, max chain len %d, hashtable load
    //    %ld/%ld %lf\n",
    //           (long) cache_obj->obj_id,
    //           (unsigned long) hv, max_chain_len,
    //           (long) hashtable->n_obj,
    //           (long) hashsize(hashtable->hashpower),
    //           (double) hashtable->n_obj / hashsize(hashtable->hashpower)
    //    );

    //    cache_obj_t *tmp_obj = hashtable->ptr_table[hv];
    //    while (tmp_obj) {
    //      printf("%ld (%d), ", (long) tmp_obj->obj_id, tmp_obj->LSC.in_cache);
    //      tmp_obj = tmp_obj->hash_next;
    //    }
    //    printf("\n");
  }

  if (cur_obj != NULL) {
    cur_obj->hash_next = cache_obj->hash_next;
    hashtable->n_obj -= 1;
    if (!hashtable->external_obj) free_cache_obj(cache_obj);
    return true;
  }
  return false;
}

/**
 *  This function is used to delete an object from the hash table by object id.
 *  - if the object is in the hash table
 *      remove it
 *      return true.
 *  - if the object is not in the hash table
 *      return false.
 *
 *  @method chained_hashtable_delete_obj_id_v2
 *  @date   2023-11-28
 *  @param  hashtable                          [Handle to the hashtable]
 *  @param  obj_id                             [The object id to remove]
 *  @return                                    [true or false]
 */
bool pchained_hashtable_delete_obj_id(hashtable_t *hashtable,
                                        const obj_id_t obj_id) {
  uint64_t hv = get_hash_value_int_64(&obj_id) & hashmask(hashtable->hashpower);
  cache_obj_t *cur_obj = hashtable->ptr_table[hv];
  // the hash bucket is empty
  if (cur_obj == NULL){
    return false;
  }

  // the object to remove is the first object in the hash bucket
  if (cur_obj->obj_id == obj_id) {
    hashtable->ptr_table[hv] = cur_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cur_obj);
    hashtable->n_obj -= 1;
    return true;
  }

  cache_obj_t *prev_obj;

  do {
    prev_obj = cur_obj;
    cur_obj = cur_obj->hash_next;
  } while (cur_obj != NULL && cur_obj->obj_id != obj_id);

  // the object to remove is in the hash bucket
  if (cur_obj != NULL) {
    prev_obj->hash_next = cur_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cur_obj);
    hashtable->n_obj -= 1;
    return true;
  }
  // the object to remove is not in the hash table
  return false;
}

cache_obj_t *pchained_hashtable_rand_obj(const hashtable_t *hashtable) {
  uint64_t pos = next_rand() & hashmask(hashtable->hashpower);
  while (hashtable->ptr_table[pos] == NULL)
    pos = next_rand() & hashmask(hashtable->hashpower);
  return hashtable->ptr_table[pos];
}

// bTD: check this function and check whether it is being referenced
// assume that iter_func has write
void pchained_hashtable_foreach(hashtable_t *hashtable,
                                  hashtable_iter iter_func, void *user_data) {
  cache_obj_t *cur_obj, *next_obj;
  for (uint64_t i = 0; i < hashsize(hashtable->hashpower); i++) {
    cur_obj = hashtable->ptr_table[i];
    while (cur_obj != NULL) {
      next_obj = cur_obj->hash_next;
      iter_func(cur_obj, user_data);
      cur_obj = next_obj;
    }
  }
}

// bTD: do we need to lock in this case?
void free_pchained_hashtable(hashtable_t *hashtable) {
  if (!hashtable->external_obj)
    pchained_hashtable_foreach(hashtable, foreach_free_obj, NULL);
  my_free(sizeof(cache_obj_t *) * hashsize(hashtable->hashpower),
          hashtable->ptr_table);
  my_free(sizeof(hashtable_t), hashtable);
}

/* grows the hashtable to the next power of 2. */
// no need for lock because it is called by other functions with lock
static void _pchained_hashtable_expand(hashtable_t *hashtable) {
  cache_obj_t **old_table = hashtable->ptr_table;
  hashtable->ptr_table =
      my_malloc_n(cache_obj_t *, hashsize(++hashtable->hashpower));
#ifdef USE_HUGEPAGE
  madvise(hashtable->table,
          sizeof(cache_obj_t *) * hashsize(hashtable->hashpower),
          MADV_HUGEPAGE);
#endif
  memset(hashtable->ptr_table, 0,
         hashsize(hashtable->hashpower) * sizeof(cache_obj_t *));
  ASSERT_NOT_NULL(hashtable->ptr_table,
                  "unable to grow hashtable to size %llu\n",
                  hashsizeULL(hashtable->hashpower));

  VERBOSE("hashtable resized from %llu to %llu\n",
          hashsizeULL((uint16_t)(hashtable->hashpower - 1)),
          hashsizeULL(hashtable->hashpower));

  cache_obj_t *cur_obj, *next_obj;
  for (uint64_t i = 0; i < hashsize((uint16_t)(hashtable->hashpower - 1));
       i++) {
    // create a dummy object for each entry
    request_t* req = new_request();
    cache_obj_t* cache_obj = create_cache_obj_from_request(req);
    hashtable->ptr_table[i] = cache_obj;
    cur_obj = old_table[i] -> hash_next;
    while (cur_obj != NULL) {
      next_obj = cur_obj->hash_next;
      cur_obj->hash_next = NULL;
      add_to_bucket(hashtable, cur_obj);
      cur_obj = next_obj;
    }
  }
  my_free(sizeof(cache_obj_t) * hashsize(hashtable->hashpower), old_table);
}

static bool check_last_bit_atomic(const atomic_int* var) {
    int value = atomic_load(var);
    return (value & 1); // Returns true if the last bit is set, false otherwise
}


void check_pchained_hashtable_integrity(const hashtable_t *hashtable) {
  cache_obj_t *cur_obj, *next_obj;
  for (uint64_t i = 0; i < hashsize(hashtable->hashpower); i++) {
    cur_obj = hashtable->ptr_table[i];
    while (cur_obj != NULL) {
      next_obj = cur_obj->hash_next;
      assert(i == (get_hash_value_int_64(&cur_obj->obj_id) &
                   hashmask(hashtable->hashpower)));
      cur_obj = next_obj;
    }
  }
}

// call inside, no need for lock
static int count_n_obj_in_bucket(cache_obj_t *curr_obj) {
  obj_id_t obj_id_arr[64];
  int chain_len = 0;
  while (curr_obj != NULL) {
    obj_id_arr[chain_len] = curr_obj->obj_id;
    for (int i = 0; i < chain_len; i++) {
      if (obj_id_arr[i] == curr_obj->obj_id) {
        ERROR("obj_id %lu is duplicated in hashtable\n",
              (unsigned long)curr_obj->obj_id);
        abort();
      }
    }

    curr_obj = curr_obj->hash_next;
    chain_len += 1;
  }

  return chain_len;
}

static void print_hashbucket_item_distribution(const hashtable_t *hashtable) {
  int n_print = 0;
  int n_obj = 0;
  for (int i = 0; i < hashsize(hashtable->hashpower); i++) {
    int chain_len = count_n_obj_in_bucket(hashtable->ptr_table[i]);
    n_obj += chain_len;
    if (chain_len > 1) {
      printf("%d, ", chain_len);
      n_print++;
    }
    if (n_print == 20) {
      printf("\n");
      n_print = 0;
    }
  }
  printf("\n #################### %d \n", n_obj);
}

static bool compare_and_set(int* ptr, int oldval, int newval) {
    // Atomically compares the contents of *ptr with oldval
    // If equal, it writes newval into *ptr
    // The function returns true if the comparison is successful and newval was written.
    return __sync_bool_compare_and_swap(ptr, oldval, newval);
}

#ifdef __cplusplus
}
#endif
