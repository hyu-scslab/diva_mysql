/*-------------------------------------------------------------------------
 *
 * ebi_tree_process.c
 *
 * EBI Tree Process Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/ebi_tree_process.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM

#include "read0types.h"

#include <signal.h>
#include <unistd.h>

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree_process.h"
#include "include/ebi_tree_buf.h"

#include <chrono>

#define EBI_DELAY_IN_MS (300)

constexpr double ebi_force_insert_delay = 2000; /* in milliseconds */
EbiTreeStruct *EbiTreePtr = nullptr;


void ebi_tree_thread() {

  using namespace std::chrono;
  
  bool first_insert = false;
  high_resolution_clock::time_point start_time, end_time;
  milliseconds current_dur;

  ReadView* back_view = UT_NEW_NOKEY(ReadView());
  while (srv_shutdown_state.load() < SRV_SHUTDOWN_CLEANUP) {

    os_event_wait_time(srv_ebi_tree_event, 1000 * EBI_DELAY_IN_MS);

    if (srv_shutdown_state.load() >= SRV_SHUTDOWN_CLEANUP) {
      UT_DELETE(back_view);
      break;
    }

    os_rmb;

    if (!QueueIsEmpty(EbiTreePtr->unlink_queue)) {
      UnlinkNodes(EbiTreePtr->ebitree, EbiTreePtr->unlink_queue);
    }

    GCQueueWork();

    if (NeedsNewNode(EbiTreePtr->ebitree)) {
      start_time = high_resolution_clock::now();
      InsertNode(EbiTreePtr->ebitree);
      back_view->background_work();
    } else if (EbiTreePtr->ebitree->recent_node->left_boundary != nullptr) {
      end_time = high_resolution_clock::now();
      current_dur = duration_cast<milliseconds>(end_time - start_time);
      if (current_dur.count() >= ebi_force_insert_delay) {
        start_time = high_resolution_clock::now();
        InsertNode(EbiTreePtr->ebitree);
        back_view->background_work();
      }
    }
  }
}

/* --------------------------------
 *		communication with backends
 * --------------------------------
 */
void
EbiTreeInit(void) {

  EbiTreePtr = (EbiTreeStruct *) ut_zalloc_nokey(sizeof(EbiTreeStruct));
	EbiTreePtr->seg_id = 1;
	EbiTreePtr->num_versions = 0;

  EbiTreePtr->ebitree = InitEbiTree();
  EbiTreePtr->unlink_queue = InitQueue();

  EbiTreeBufInit();
  EbiTreeGCQueueInit();
}

void
EbiTreeFree(void) {
  EbiTreeGCQueueFree();
  EbiTreeBufFree();
  ut_free(EbiTreePtr);
}

#endif
