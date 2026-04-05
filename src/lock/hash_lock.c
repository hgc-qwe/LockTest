#include "hash_lock.h"

#include <stdio.h>
#include <stdlib.h>

void hashInit(hash_lock_t* bucket) {
  for (int i = 0; i < HASHNUM; i++) {
    bucket->table[i].head = NULL;
    pthread_mutex_init(&bucket->table[i].mutex, NULL);
  }
}

int getValue(hash_lock_t* bucket, int key) {
  int idx = HASH(key);
  pthread_mutex_lock(&bucket->table[idx].mutex);

  Hnode* p = bucket->table[idx].head;
  while (p) {
    if (p->key == key) {
      int val = p->value;
      pthread_mutex_unlock(&bucket->table[idx].mutex);
      return val;
    }
    p = p->next;
  }

  pthread_mutex_unlock(&bucket->table[idx].mutex);
  return -1;
}

void insert(hash_lock_t* bucket, int key, int value) {
  int idx = HASH(key);

  pthread_mutex_lock(&bucket->table[idx].mutex);
  Hlist cur = bucket->table[idx].head;
  Hlist prev = NULL;
  while(cur){
    if(cur->key == key){
      cur->value = value;
      pthread_mutex_unlock(&bucket->table[idx].mutex);
      return;
    }
    prev = cur;
    cur = cur->next;
  }

  if(!cur){
    Hnode* node = (Hnode*)malloc(sizeof(Hnode));
    node->value = value;
    node->key = key;
    node->next = bucket->table[idx].head;
    bucket->table[idx].head = node;
  }
  pthread_mutex_unlock(&bucket->table[idx].mutex);
}

int setKey(hash_lock_t* bucket, int key, int new_key) {
  int idx = HASH(key);
  int new_idx = HASH(new_key);

  if (idx == new_idx) {
    pthread_mutex_lock(&bucket->table[idx].mutex);
  } else {
    if (idx < new_idx) {
      pthread_mutex_lock(&bucket->table[idx].mutex);
      pthread_mutex_lock(&bucket->table[new_idx].mutex);
    } else {
      pthread_mutex_lock(&bucket->table[new_idx].mutex);
      pthread_mutex_lock(&bucket->table[idx].mutex);
    }
  }

  Hnode* cur = bucket->table[idx].head;
  Hnode* prev = NULL;

  while (cur) {
    if (cur->key == key) break;
    prev = cur;
    cur = cur->next;
  }

  if (!cur) {
    if (idx != new_idx) {
      pthread_mutex_unlock(&bucket->table[idx].mutex);
      pthread_mutex_unlock(&bucket->table[new_idx].mutex);
    } else {
      pthread_mutex_unlock(&bucket->table[idx].mutex);
    }
    return -1;
  }

  if (prev == NULL) {
    bucket->table[idx].head = cur->next;
  } else {
    prev->next = cur->next;
  }
  cur->key = new_key;
  cur->next = bucket->table[new_idx].head;
  bucket->table[new_idx].head = cur;

  if (idx != new_idx) {
    pthread_mutex_unlock(&bucket->table[idx].mutex);
    pthread_mutex_unlock(&bucket->table[new_idx].mutex);
  } else {
    pthread_mutex_unlock(&bucket->table[idx].mutex);
  }

  return 0;
}