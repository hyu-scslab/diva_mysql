/*****************************************************************************

Copyright (c) 1994, 2020, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2021, Anonymous Lab.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file pleaf/pleaf_mgr.cc
 P-Leaf Generator Thread Interfaces

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifdef J3VM

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
