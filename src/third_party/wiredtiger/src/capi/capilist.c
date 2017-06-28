#include "wt_internal.h"
#include <sys/errno.h>
#include <math.h>
#include <stdlib.h>

/*Simple doubly linked list implementation to store free segments*/

/*Node initialization*/
WT_CAPI_FREELIST_NODE*
__wt_capi_initNode(WT_CAPI_SEGMENT *segment)
{
  WT_CAPI_FREELIST_NODE *node = malloc(sizeof(WT_CAPI_FREELIST_NODE));
  node->item = segment;
  node->next = NULL;
  node->prev = NULL;
  return (node);
}

/*List initialization*/
void
__wt_capi_initList(WT_CAPI_FREELIST *list)
{
  list->head=NULL;
  list->tail=NULL;
  list->size=0;
}

/*Insert back of the list*/
void
__wt_capi_insertBack(WT_CAPI_FREELIST *list, WT_CAPI_SEGMENT *segment)
{
  WT_CAPI_FREELIST_NODE *node = __wt_capi_initNode(segment);
  /*if empty tail and head are pointing to the first node*/
  if (list->head == NULL) {
    list->tail = node;
    list->head = node;
    node->next = NULL;
    node->prev = NULL;
  }else{
    list->tail->next = node;
    node->prev = list->tail;
    list->tail = node;
  }
  list->size++;
}

/*Remove from the front*/
WT_CAPI_SEGMENT*
__wt_capi_deleteFirst(WT_CAPI_FREELIST *list)
{
  WT_CAPI_FREELIST_NODE *node = list->head;
  if (list->head == NULL) {
    return (NULL);
  }
  if (list->head == list->tail) {
    list->head=NULL;
    list->tail=NULL;
  }else{
    list->head->next->prev=NULL;
    list->head = list->head->next;
  }
  list->size--;
  WT_CAPI_SEGMENT *temp;
  temp = node->item;
  free(node);
  return (temp);
}

/*Checks if the list has items*/
bool
__wt_capi_isEmpty(WT_CAPI_FREELIST *list)
{
  return (list->size==0);
}

/*Deletes the item from the list with given pointer*/
WT_CAPI_SEGMENT*
__wt_capi_deleteItem(WT_CAPI_FREELIST *list,WT_CAPI_FREELIST_NODE *ptr)
{
  if(ptr==list->head){
    return __wt_capi_deleteFirst(list);
  }

  if(ptr!=list->tail){
    ptr->next->prev=ptr->prev;
  }

  ptr->prev->next=ptr->next;
  list->size--;
  WT_CAPI_SEGMENT *temp;
  temp = ptr->item;
  free(ptr);
  return (temp);
}


