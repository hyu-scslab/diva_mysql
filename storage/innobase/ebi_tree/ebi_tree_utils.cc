/*-------------------------------------------------------------------------
 *
 * ebi_tree_utils.c
 *
 * Data Structures for EBI Tree Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ebi_tree/ebi_tree_utils.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM

#include "read0types.h"

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree.h"

static TaskNode AllocTask(EbiNode node);

TaskQueue InitQueue() {
  TaskQueue queue;
  TaskNode sentinel;

  queue = (TaskQueue) ut_zalloc_nokey(sizeof(TaskQueueStruct));
  sentinel = AllocTask(nullptr);

  queue->head = queue->tail = sentinel;
  return queue;
}

void FreeQueue(TaskQueue queue) {
  if (queue) {
    while (Dequeue(queue) != nullptr) {};

    ut_a(queue->head);
    ut_free(queue->head);
    ut_free(queue);
  }
}

static TaskNode AllocTask(EbiNode node) {
  TaskNode task;

  task = (TaskNode) ut_zalloc_nokey(sizeof(TaskNodeStruct));
  task->node = node;
  task->next = nullptr;

  return task;
}

bool QueueIsEmpty(TaskQueue queue) {
  return (queue->tail == queue->head);
}

void Enqueue(TaskQueue queue, EbiNode node) {
  TaskNode tail;
  TaskNode new_tail;
  bool success;

  new_tail = AllocTask(node);

  success = false;

  while (!success) {
    // JAESEON: expected ?? 
    tail = queue->tail;

    // Try logical enqueue, not visible to the dequeuer.
    success = __sync_bool_compare_and_swap(&tail->next, nullptr, new_tail);

    // Physical enqueue.
    if (success) {
      // The thread that succeeded in changing the tail is responsible for the
      // physical enqueue. Other threads that fail might retry the loop, but
      // the ones that read the tail before the tail is changed will fail on
      // calling CAS since the next pointer is not nullptr. Thus, only the
      // threads that read the tail after the new tail assignment will be
      // competing for logical enqueue.
      queue->tail = new_tail;
    } else {
      // Instead of retrying right away, calling yield() will save the CPU
      // from wasting cycles.
      // TODO: uncomment
      // pthread_yield();
    }
  }
}

EbiNode Dequeue(TaskQueue queue) {
  TaskNode head;
  EbiNode ret;

  head = queue->head;

  if (!head->next)
    return nullptr;

  ret = head->next->node;
  
  queue->head = head->next;

  ut_free(head);
  return (ret);
}

#endif /* J3VM */
