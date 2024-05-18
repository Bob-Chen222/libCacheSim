

#include <assert.h>
#include <gmodule.h>

#include "../include/libCacheSim/cacheObj.h"
#include "../include/libCacheSim/macro.h"
#include "../include/libCacheSim/request.h"

/**
 * copy the cache_obj to req_dest
 * @param req_dest
 * @param cache_obj
 */
void copy_cache_obj_to_request(request_t *req_dest,
                               const cache_obj_t *cache_obj) {
  req_dest->obj_id = cache_obj->obj_id;
  req_dest->obj_size = cache_obj->obj_size;
  req_dest->next_access_vtime = cache_obj->misc.next_access_vtime;
  req_dest->valid = true;
}

/**
 * copy the data from request into cache_obj
 * @param cache_obj
 * @param req
 */
void copy_request_to_cache_obj(cache_obj_t *cache_obj, const request_t *req) {
  cache_obj->obj_size = req->obj_size;
#ifdef SUPPORT_TTL
  if (req->ttl != 0)
    cache_obj->exp_time = req->clock_time + req->ttl;
  else
    cache_obj->exp_time = 0;
#endif
  cache_obj->obj_id = req->obj_id;
}

/**
 * create a cache_obj from request
 * @param req
 * @return
 */
cache_obj_t *create_cache_obj_from_request(const request_t *req) {
  cache_obj_t *cache_obj = my_malloc(cache_obj_t);
  memset(cache_obj, 0, sizeof(cache_obj_t));
  if (req != NULL) copy_request_to_cache_obj(cache_obj, req);
  return cache_obj;
}

/** remove the object from the built-in doubly linked list
 *
 * @param head
 * @param tail
 * @param cache_obj
 */
void remove_obj_from_list(cache_obj_t **head, cache_obj_t **tail,
                          cache_obj_t *cache_obj) {

  assert(cache_obj != NULL);
  if (head != NULL && cache_obj == *head) {
    *head = cache_obj->queue.next;
    if (cache_obj->queue.next != NULL) cache_obj->queue.next->queue.prev = NULL;
  }
  if (tail != NULL && cache_obj == *tail) {

    
    *tail = cache_obj->queue.prev;
    if (cache_obj->queue.prev != NULL) cache_obj->queue.prev->queue.next = NULL;
  }

  if (cache_obj->queue.prev != NULL)
    cache_obj->queue.prev->queue.next = cache_obj->queue.next;

  if (cache_obj->queue.next != NULL)
    cache_obj->queue.next->queue.prev = cache_obj->queue.prev;

  cache_obj->queue.prev = NULL;
  cache_obj->queue.next = NULL;
}

/**
 * move an object to the tail of the doubly linked list
 * @param head
 * @param tail
 * @param cache_obj
 */
void move_obj_to_tail(cache_obj_t **head, cache_obj_t **tail,
                      cache_obj_t *cache_obj) {
  if (*head == *tail) {
    // the list only has one element
    assert(cache_obj == *head);
    assert(cache_obj->queue.next == NULL);
    assert(cache_obj->queue.prev == NULL);
    return;
  }
  if (cache_obj == *head) {
    // change head
    *head = cache_obj->queue.next;
    cache_obj->queue.next->queue.prev = NULL;

    // move to tail
    (*tail)->queue.next = cache_obj;
    cache_obj->queue.next = NULL;
    cache_obj->queue.prev = *tail;
    *tail = cache_obj;
    return;
  }
  if (cache_obj == *tail) {
    return;
  }

  // bridge list_prev and next
  cache_obj->queue.prev->queue.next = cache_obj->queue.next;
  cache_obj->queue.next->queue.prev = cache_obj->queue.prev;

  // handle current tail
  (*tail)->queue.next = cache_obj;

  // handle this moving object
  cache_obj->queue.next = NULL;
  cache_obj->queue.prev = *tail;

  // handle tail
  *tail = cache_obj;
}

/**
 * move an object to the head of the doubly linked list
 * @param head
 * @param tail
 * @param cache_obj
 */
void move_obj_to_head(cache_obj_t **head, cache_obj_t **tail,
                      cache_obj_t *cache_obj) {
  DEBUG_ASSERT(head != NULL);
  // DEBUG_ASSERT(contains_object(*head, cache_obj));

  // if (tail != NULL && *head == *tail) {
  //   // the list only has one element
  //   DEBUG_ASSERT(cache_obj == *head);
  //   DEBUG_ASSERT(cache_obj->queue.next == NULL);
  //   DEBUG_ASSERT(cache_obj->queue.prev == NULL);
  //   return;
  // }

  if (cache_obj == *head) {
    // already at head
    return;
  }

  if (tail != NULL && cache_obj == *tail) {
    // change tail
    cache_obj->queue.prev->queue.next = cache_obj->queue.next;
    *tail = cache_obj->queue.prev;

    // move to head
    (*head)->queue.prev = cache_obj;
    cache_obj->queue.prev = NULL;
    cache_obj->queue.next = *head;
    *head = cache_obj;
    return;
  }

  // bridge list_prev and next
  cache_obj->queue.prev->queue.next = cache_obj->queue.next;
  cache_obj->queue.next->queue.prev = cache_obj->queue.prev;

  // handle current head
  (*head)->queue.prev = cache_obj;

  // handle this moving object
  cache_obj->queue.prev = NULL;
  cache_obj->queue.next = *head;

  // handle head
  *head = cache_obj;
}

/**
 * prepend the object to the head of the doubly linked list
 * the object is not in the list, otherwise, use move_obj_to_head
 * @param head
 * @param tail
 * @param cache_obj
 */
void prepend_obj_to_head(cache_obj_t **head, cache_obj_t **tail,
                         cache_obj_t *cache_obj) {
  assert(head != NULL);

  cache_obj->queue.prev = NULL;
  cache_obj->queue.next = *head;

  if (tail != NULL && *tail == NULL) {
    // the list is empty
    DEBUG_ASSERT(*head == NULL);
    *tail = cache_obj;
  }

  if (*head != NULL) {
    // the list has at least one element
    (*head)->queue.prev = cache_obj;
  }

  *head = cache_obj;
}

void T_prepend_obj_to_head(cache_obj_t **head, cache_obj_t **tail,
                         cache_obj_t *cache_obj) {
  assert(head != NULL);
  cache_obj_t* old_head;
  do{
    old_head = *head;
    cache_obj->queue.prev = NULL;
    cache_obj->queue.next = old_head;
  } while(!__atomic_compare_exchange_n(head, &old_head, cache_obj, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
  old_head->queue.prev = cache_obj;

  if (tail != NULL && *tail == NULL) {
    // the list is empty
    DEBUG_ASSERT(*head == NULL);
    *tail = cache_obj;
  }

  if (*head != NULL) {
    // the list has at least one element
    (*head)->queue.prev = cache_obj;
  }

  *head = cache_obj;
}

/**
 * append the object to the tail of the doubly linked list
 * the object is not in the list, otherwise, use move_obj_to_tail
 * @param head
 * @param tail
 * @param cache_obj
 */
void append_obj_to_tail(cache_obj_t **head, cache_obj_t **tail,
                        cache_obj_t *cache_obj) {

  cache_obj->queue.next = NULL;
  cache_obj->queue.prev = *tail;

  if (head != NULL && *head == NULL) {
    // the list is empty
    DEBUG_ASSERT(*tail == NULL);
    *head = cache_obj;
  }

  if (*tail != NULL) {
    // the list has at least one element
    (*tail)->queue.next = cache_obj;
  }


  *tail = cache_obj;
}

cache_obj_t* T_evict_last_obj(cache_obj_t **head, cache_obj_t **tail) {
  cache_obj_t *old_tail;
  cache_obj_t *new_tail;
  do{
    old_tail = (*tail);
    // __atomic_store_n(&old_tail, *tail, __ATOMIC_RELAXED);
    // DEBUG_ASSERT(old_tail != NULL);
    new_tail = old_tail->queue.prev;
    __atomic_store_n(&new_tail, old_tail->queue.prev, __ATOMIC_RELAXED);
  }while(!__atomic_compare_exchange_n(tail, &old_tail, new_tail, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
  if (new_tail != NULL) {
    new_tail->queue.next = NULL;
  } else {
    ERROR("evicting the last object in the list\n");
    *head = NULL;
  }
  // unlock
  return old_tail;
}

void print_list(cache_obj_t *head, cache_obj_t *tail) {
  cache_obj_t *p = head;
  printf("head: %ld, tail: %ld\n", head->obj_id, tail->obj_id);
  while (p != NULL) {
    printf("%ld->", p->obj_id);
    p = p->queue.next;
  }
}

bool contains_duplicates(cache_obj_t *head){
  if (head == NULL || head->queue.next == NULL){
    return true;
  }
  cache_obj_t *p = head;
  cache_obj_t *q = head -> queue.next;

  for (; p != NULL; p = p -> queue.next){
    for (q = p -> queue.next; q != NULL; q = q -> queue.next){
      if (p -> obj_id == q -> obj_id){
        return true;
      }
    }
  }
  return false;
}

bool contains_object(cache_obj_t *head, cache_obj_t *obj){
  cache_obj_t *p = head;
  for (; p != NULL; p = p -> queue.next){
    if (p == obj){
      return true;
    }
  }
  return false;
}

// bool is_loop(cache_obj_t *head, cache_obj_t *cur) {
//   cache_obj_t *slow = head;
//   cache_obj_t *fast = head;
//   while (fast != NULL && fast->hash_f_next != NULL) {
//     slow = slow->hash_f_next;
//     fast = fast->hash_f_next->hash_f_next;
//     if (slow == cur) {
//       return true;
//     }
//   }
//   return false;
// }

bool is_doublyll_intact(cache_obj_t *head, cache_obj_t *tail){
  // check whether linked list is consistent
  cache_obj_t *p = head;
  cache_obj_t *q = tail;
  while (p -> queue.next != NULL){
    // check the link
    if (p->queue.next->queue.prev != p){
      return false;
    }
    p = p->queue.next;
  }

  while (q -> queue.prev != NULL){
    // check the link
    if (q->queue.prev->queue.next != q){
      return false;
    }
    q = q->queue.prev;
  }

  return true;
}