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
#include "chainedHashTableV2.h"

#define OBJ_EMPTY(cache_obj) ((cache_obj)->obj_size == 0)
#define NEXT_OBJ(cur_obj) (((cache_obj_t *)(cur_obj))->hash_next)

static void _chained_hashtable_expand_v2(hashtable_t *hashtable);
static void print_hashbucket_item_distribution(const hashtable_t *hashtable);
static uint64_t test_and_set(uint64_t *dummy);
static void test_and_test_and_set(uint64_t* dummy);
// static double gettime(void);

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
static inline cache_obj_t* add_to_bucket(hashtable_t *hashtable,
                                 const request_t *req) {
  cache_obj_t *cache_obj = create_cache_obj_from_request(req);
  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);

  uint64_t *dummy = &(hashtable->ptr_table[hv]->obj_id);

  test_and_test_and_set(dummy);
  for (cache_obj_t *cur_obj = hashtable->ptr_table[hv] -> hash_next; cur_obj != NULL; cur_obj = cur_obj->hash_next) {
    if (cur_obj->obj_id == cache_obj->obj_id) {
      hashtable->ptr_table[hv] -> obj_id = UINT64_MAX;
      return NULL;
    }
  }
  if (hashtable->ptr_table[hv] -> hash_next == NULL) {
    hashtable->ptr_table[hv] -> hash_next = cache_obj;
    hashtable->ptr_table[hv] -> obj_id = UINT64_MAX;
    return cache_obj;
  }
  cache_obj_t *head_ptr = hashtable->ptr_table[hv] -> hash_next;

  cache_obj->hash_next = head_ptr;
  hashtable->ptr_table[hv]->hash_next = cache_obj;
  hashtable->ptr_table[hv] -> obj_id = UINT64_MAX;
  return cache_obj;


#ifdef HASHTABLE_DEBUG
  cache_obj_t *curr_obj = cachec_obj->hash_next;
  while (curr_obj) {
    assert(curr_obj->obj_id != cache_obj->obj_id);
    curr_obj = curr_obj->hash_next;
  }
#endif
}

static inline cache_obj_t* add_obj_to_bucket(hashtable_t *hashtable,
                                 cache_obj_t *cache_obj) {
  //this function is only used for FrozenHot hashtable
  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);

  if (hashtable->ptr_table[hv]->hash_f_next == NULL) {
    hashtable->ptr_table[hv] -> hash_f_next = cache_obj;
    return cache_obj;
  }
  cache_obj_t *head_ptr = hashtable->ptr_table[hv] -> hash_f_next;

  cache_obj->hash_f_next = head_ptr;
  hashtable->ptr_table[hv]->hash_f_next = cache_obj;
  // // DEBUG_ASSERT(is_loop(head_ptr, cache_obj) == false);
  return cache_obj;
}

/* free object, called by other functions when iterating through the hashtable
 */
static inline void foreach_free_obj(cache_obj_t *cache_obj, void *user_data) {
  my_free(sizeof(cache_obj_t), cache_obj);
}

/************************ hashtable func ************************/
hashtable_t *create_chained_hashtable_v2(const uint16_t hashpower) {
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
  for (uint64_t i = 0; i < hashsize(hashtable->hashpower); i++) {
    request_t* req = new_request();
    req->obj_id = UINT64_MAX;
    cache_obj_t* cache_obj = create_cache_obj_from_request(req);
    hashtable->ptr_table[i] = cache_obj;
    free_request(req);
  }
  return hashtable;
}



cache_obj_t *chained_hashtable_find_obj_id_v2(const hashtable_t *hashtable,
                                              const obj_id_t obj_id) {

  // we will use the same lock
  DEBUG_ASSERT(obj_id != UINT64_MAX);
  DEBUG_ASSERT(obj_id != 0);
  cache_obj_t *cache_obj = NULL;
  uint64_t hv = get_hash_value_int_64(&obj_id);
  hv = hv & hashmask(hashtable->hashpower);
  

  cache_obj = hashtable->ptr_table[hv] -> hash_next;

  while (cache_obj) {
    if (cache_obj->obj_id == obj_id) {
      // hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
      return cache_obj;
    }
    cache_obj = cache_obj->hash_next;
  }
  // hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
  return cache_obj;
}

cache_obj_t *chained_hashtable_find_v2(const hashtable_t *hashtable,
                                       const request_t *req) {
  cache_obj_t* obj = chained_hashtable_find_obj_id_v2(hashtable, req->obj_id);
  return obj;
}

cache_obj_t *chained_hashtable_find_obj_v2(const hashtable_t *hashtable,
                                           const cache_obj_t *obj_to_find) {

  return chained_hashtable_find_obj_id_v2(hashtable, obj_to_find->obj_id);
}

/* the user needs to make sure the added object is not in the hash table */
cache_obj_t *chained_hashtable_insert_v2(hashtable_t *hashtable,
                                         const request_t *req) {
  cache_obj_t *new_cache_obj = add_to_bucket(hashtable, req);
  if (new_cache_obj) {
    DEBUG_ASSERT(new_cache_obj->obj_id != 0);
  }
  return new_cache_obj;
}

cache_obj_t *chained_hashtable_insert_obj_v2(hashtable_t *hashtable,
                                         cache_obj_t *cache_obj) {
  DEBUG_ASSERT(cache_obj != NULL);
  cache_obj_t *new_cache_obj = add_obj_to_bucket(hashtable, cache_obj);
  if (new_cache_obj) {
    DEBUG_ASSERT(new_cache_obj->obj_id != 0);
  }
  return new_cache_obj;
}
/* you need to free the extra_metadata before deleting from hash table */
void chained_hashtable_delete_v2(hashtable_t *hashtable,
                                 cache_obj_t *cache_obj) {

  DEBUG_ASSERT(cache_obj != NULL);
  // first check whether the whole hashtable is in the process of expanding
  //at this moment the expand completes
  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);
  uint64_t *dummy = &(hashtable->ptr_table[hv]->obj_id);
  test_and_test_and_set(dummy);
  if ((hashtable->ptr_table[hv] -> hash_next) == cache_obj) {
    hashtable->ptr_table[hv]->hash_next = cache_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cache_obj);
    hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
    return;
  }

  static int max_chain_len = 16;
  int chain_len = 1;
  cache_obj_t *cur_obj = hashtable->ptr_table[hv] -> hash_next;
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
  if (cur_obj == NULL) {
    printf("cur_obj is null\n");
    printf("obj_id %lu\n", (unsigned long)cache_obj->obj_id);
  }
  cur_obj->hash_next = cache_obj->hash_next;
  if (!hashtable->external_obj) {
    free_cache_obj(cache_obj);
  }

  hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
}

bool chained_hashtable_try_delete_v2(hashtable_t *hashtable,
                                     cache_obj_t *cache_obj) {
  static int max_chain_len = 1;

  uint64_t hv = get_hash_value_int_64(&cache_obj->obj_id) &
                hashmask(hashtable->hashpower);


  uint64_t *dummy = &(hashtable->ptr_table[hv]->obj_id);
  uint64_t old = UINT64_MAX;
  uint64_t new = UINT64_MAX - 1;
  while (__atomic_compare_exchange(dummy, &old, &new, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED) == 0) {
    old = UINT64_MAX;
  }

  if (hashtable->ptr_table[hv] -> hash_next == cache_obj) {
    hashtable->ptr_table[hv] -> hash_next = cache_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cache_obj);
    hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
    return true;
  }

  int chain_len = 1;
  cache_obj_t *cur_obj = hashtable->ptr_table[hv] -> hash_next;
  while (cur_obj != NULL && cur_obj->hash_next != cache_obj) {
    cur_obj = cur_obj->hash_next;
    chain_len += 1;
  }

  if (chain_len > 16 && chain_len > max_chain_len) {
    max_chain_len = chain_len;
      //  WARN("hashtable remove %ld, hv %lu, max chain len %d, hashtable load
      //  %ld/%ld %lf\n",
      //         (long) cache_obj->obj_id,
      //         (unsigned long) hv, max_chain_len,
      //         (long) hashtable->n_obj,
      //         (long) hashsize(hashtable->hashpower),
      //         (double) hashtable->n_obj / hashsize(hashtable->hashpower)
      //  );

      //  cache_obj_t *tmp_obj = hashtable->ptr_table[hv];
      //  while (tmp_obj) {
      //    printf("%ld (%d), ", (long) tmp_obj->obj_id, tmp_obj->LSC.in_cache);
      //    tmp_obj = tmp_obj->hash_next;
      //  }
      //  printf("\n");
  }

  if (cur_obj != NULL) {
    cur_obj->hash_next = cache_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cache_obj);
    hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
    return true;
  }

  hashtable->ptr_table[hv]->obj_id = UINT64_MAX;
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
bool chained_hashtable_delete_obj_id_v2(hashtable_t *hashtable,
                                        const obj_id_t obj_id) {
  uint64_t hv = get_hash_value_int_64(&obj_id) & hashmask(hashtable->hashpower);
  cache_obj_t *cur_obj = hashtable->ptr_table[hv];
  // the hash bucket is empty
  if (cur_obj == NULL) return false;

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

  if (cur_obj != NULL) {
    prev_obj->hash_next = cur_obj->hash_next;
    if (!hashtable->external_obj) free_cache_obj(cur_obj);
    hashtable->n_obj -= 1;
    return true;
  }
  return false;
}

cache_obj_t *chained_hashtable_rand_obj_v2(const hashtable_t *hashtable) {
  // use the same lock

  uint64_t pos = next_rand() & hashmask(hashtable->hashpower);
  while (hashtable->ptr_table[pos]->hash_next == NULL)
    pos = next_rand() & hashmask(hashtable->hashpower);
  // add readlock
  uint64_t *dummy = &(hashtable->ptr_table[pos]->obj_id);
  uint64_t old = UINT64_MAX;
  uint64_t new = UINT64_MAX - 1;
  cache_obj_t *cache_obj = hashtable->ptr_table[pos]->hash_next;
  if (cache_obj == NULL){
    hashtable->ptr_table[pos]->obj_id = UINT64_MAX;
    return chained_hashtable_rand_obj_v2(hashtable);
  }else{
    hashtable->ptr_table[pos]->obj_id = UINT64_MAX;
    return cache_obj;
  }
}

void chained_hashtable_foreach_v2(hashtable_t *hashtable,
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





void free_chained_hashtable_v2(hashtable_t *hashtable) {
  if (!hashtable->external_obj)
    chained_hashtable_foreach_v2(hashtable, foreach_free_obj, NULL);
  my_free(sizeof(cache_obj_t *) * hashsize(hashtable->hashpower),
          hashtable->ptr_table);
  my_free(sizeof(hashtable_t), hashtable);
}




void check_hashtable_integrity_v2(const hashtable_t *hashtable) {
  printf("no call integrity\n");
  cache_obj_t *cur_obj, *next_obj;
  uint64_t mask = ~(1);
  for (uint64_t i = 0; i < hashsize(hashtable->hashpower); i++) {
    cur_obj = ((cache_obj_t*)((uint64_t)hashtable->ptr_table[i] & mask)) -> hash_next;
    while (cur_obj != NULL) {
      next_obj = cur_obj->hash_next;
      assert(i == (get_hash_value_int_64(&cur_obj->obj_id) &
                   hashmask(hashtable->hashpower)));
      cur_obj = next_obj;
    }
  }
}

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



static inline void foreach_verify_obj(cache_obj_t *cache_obj, void *user_data) {
  DEBUG_ASSERT(cache_obj->obj_id != 0);
  cache_obj_t* start = (cache_obj_t*)user_data;
  if (cache_obj->obj_id != UINT64_MAX) {
    DEBUG_ASSERT(contains_object(start, cache_obj));
  }
}


static inline void foreach_free_hash_f_next(cache_obj_t *cache_obj, void *user_data) {
  cache_obj -> hash_f_next = NULL;
}


void verify_objects_hashtable_v2(hashtable_t *hashtable, cache_obj_t *head) {
  chained_hashtable_foreach_v2(hashtable, foreach_verify_obj, head);
}

void chained_hashtable_f_foreach_v2(hashtable_t *hashtable,
                                  hashtable_iter iter_func, void *user_data) {
  cache_obj_t *cur_obj, *next_obj;
  for (uint64_t i = 0; i < hashsize(hashtable->hashpower); i++) {
    cur_obj = hashtable->ptr_table[i];
    while (cur_obj != NULL) {
      next_obj = cur_obj->hash_f_next;
      iter_func(cur_obj, user_data);
      cur_obj = next_obj;
    }
  }
}

void free_chained_hashtable_f_v2(hashtable_t *hashtable) {
  if (!hashtable->external_obj)
    chained_hashtable_f_foreach_v2(hashtable, foreach_free_hash_f_next, NULL);
}

bool is_loop(cache_obj_t *head, cache_obj_t *cur) {
  cache_obj_t *slow = head;
  cache_obj_t *fast = head;
  while (fast != NULL && fast->hash_f_next != NULL) {
    slow = slow->hash_f_next;
    fast = fast->hash_f_next->hash_f_next;
    if (slow == cur) {
      return true;
    }
  }
  return false;
}

bool is_loop_bp(cache_obj_t *head, cache_obj_t *cur) {
  cache_obj_t *slow = head;
  cache_obj_t *fast = head;
  while (fast != NULL && fast->hash_next != NULL) {
    slow = slow->hash_next;
    fast = fast->hash_next->hash_next;
    if (slow == cur) {
      return true;
    }
  }
  return false;
}


cache_obj_t *chained_hashtable_f_find_obj_id_v2(const hashtable_t *hashtable,
                                              const obj_id_t obj_id) {

  // we will use the same lock
  DEBUG_ASSERT(obj_id != UINT64_MAX);
  DEBUG_ASSERT(obj_id != 0);
  cache_obj_t *cache_obj = NULL;
  uint64_t hv = get_hash_value_int_64(&obj_id);
  hv = hv & hashmask(hashtable->hashpower);

  cache_obj = hashtable->ptr_table[hv] -> hash_f_next;

  // DEBUG_ASSERT(is_loop(cache_obj, NULL) == false);

  while (cache_obj) {
    if (cache_obj->obj_id == obj_id) {
      return cache_obj;
    }
    cache_obj = cache_obj->hash_f_next;
  }
  return cache_obj;
}

static uint64_t test_and_set(uint64_t *dummy) {
    uint64_t expected = UINT64_MAX;
    uint64_t new = UINT64_MAX - 1;
    return __atomic_compare_exchange(dummy, &expected, &new, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
}

static void test_and_test_and_set(unsigned long* dummy) {
    while (!test_and_set(dummy)) {
        // Busy wait if the lock is taken
        while (__atomic_load_n(dummy, __ATOMIC_RELAXED) == UINT64_MAX - 1) {
            // Lock is busy, just wait
        }
    }
}

#ifdef __cplusplus
}
#endif