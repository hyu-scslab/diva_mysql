/*-------------------------------------------------------------------------
 *
 * ebi_tree_utils.h
 *    Includes multiple data structures such as,
 *      - Multiple Producer Single Consumer Queue (MPSC queue)
 *	    - Linked List
 *
 *
 *
 * src/include/storage/ebi_tree_utils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_UTILS_H
#define EBI_TREE_UTILS_H

#include "univ.i"

typedef struct EbiNodeData* EbiNode;

/*
 * MPSC Queue
 */
typedef struct TaskNodeStruct* TaskNode;

typedef struct TaskNodeStruct {
  EbiNode node;
  TaskNode next;
} TaskNodeStruct;


typedef struct TaskQueueStruct* TaskQueue;

typedef struct TaskQueueStruct {
  TaskNode head;
  TaskNode tail;
} TaskQueueStruct;


extern TaskQueue InitQueue();
extern void DeleteQueue(TaskQueue queue);
extern bool QueueIsEmpty(TaskQueue queue);
extern void Enqueue(TaskQueue queue, EbiNode node);
extern EbiNode Dequeue(TaskQueue queue);

#endif /* EBI_TREE_UTILS_H */
