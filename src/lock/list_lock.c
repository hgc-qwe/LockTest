#include "list_lock.h"

#include <stdio.h>
#include <stdlib.h>

void listInit(list_lock_t* list) {
  list->head = NULL;
  pthread_mutex_init(&list->mutex, NULL);
  pthread_cond_init(&list->cond, NULL);
}

void producer(list_lock_t* list, DataType value) {
  LNode* node = (LNode*)malloc(sizeof(LNode));
  node->value = value;

  pthread_mutex_lock(&list->mutex);
  node->next = list->head;
  list->head = node;
  pthread_cond_signal(&list->cond);
  pthread_mutex_unlock(&list->mutex);
}

void consumer(list_lock_t* list) {
  pthread_mutex_lock(&list->mutex);

  while (list->head == NULL) {
    pthread_cond_wait(&list->cond, &list->mutex);
  }

  LNode* node = list->head;
  list->head = node->next;

  pthread_mutex_unlock(&list->mutex);
  free(node);
}

int getListSize(list_lock_t* list) {
  pthread_mutex_lock(&list->mutex);

  int size = 0;
  LNode* p = list->head;
  while (p) {
    size++;
    p = p->next;
  }

  pthread_mutex_unlock(&list->mutex);
  return size;
}