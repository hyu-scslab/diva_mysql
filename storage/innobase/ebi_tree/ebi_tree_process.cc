/*-------------------------------------------------------------------------
 *
 * ebi_tree_process.cc
 *
 * EBI Tree Process Implementation
 *
 *
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA

#include "read0types.h"

#include <signal.h>
#include <unistd.h>

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree_process.h"
#include "include/ebi_tree_buf.h"

#include <chrono>

#define EBI_DELAY_IN_MS (100)

constexpr double ebi_force_insert_delay = 100; /* in milliseconds */
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
      //back_view->background_work();
    } else if (EbiTreePtr->ebitree->recent_node->left_boundary != nullptr) {
      end_time = high_resolution_clock::now();
      current_dur = duration_cast<milliseconds>(end_time - start_time);
      if (current_dur.count() >= ebi_force_insert_delay) {
        start_time = high_resolution_clock::now();
        InsertNode(EbiTreePtr->ebitree);
        //back_view->background_work();
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
