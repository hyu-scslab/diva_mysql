/*-------------------------------------------------------------------------
 *
 * pleaf_mgr.cc
 * 		Provisional Index Manager Implementation 
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

#include "srv0srv.h"
#include "read0types.h"

#include <signal.h>
#include <unistd.h>
#include <time.h>

#include "include/pleaf_stack_helper.h"
#include "include/pleaf_bufpage.h"
#include "include/pleaf_buf.h"
#include "include/pleaf_hash.h"
#include "include/pleaf_mgr.h"

#define PLEAF_DELAY_IN_MS (1000)

static bool started {false};
static ulint tup_len {0};

void pleaf_generator_thread() {
  while (srv_shutdown_state.load() < SRV_SHUTDOWN_CLEANUP) {

    os_event_wait_time(srv_pleaf_generator_event, 1000 * PLEAF_DELAY_IN_MS);

    if (srv_shutdown_state.load() >= SRV_SHUTDOWN_CLEANUP)
      break;

    os_rmb;
		PLeafCleanOldGeneration();

    /* Do work */
		if (PLeafNeedsNewGeneration())
		{
      /* Create new generation */
			PLeafMakeNewGeneration();
		}

    if (started)
      MonitorOurs(tup_len);
  }
}

/* for temp */
void pleaf_monitor_on(ulint tuple_size) {
  if (!started) {
    tup_len = tuple_size;
    started = true;
  } else
    return;
}
#endif
