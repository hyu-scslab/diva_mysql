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

/** @file pleaf/pleaf_hash.cc
 P-Leaf Hash Interfaces

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifdef J3VM

#include "read0types.h"
#include "ha0ha.h"

#include "include/pleaf_stack_helper.h"
#include "include/pleaf_bufpage.h"
#include "include/pleaf_buf.h"
#include "include/pleaf_hash.h"

typedef struct
{
  PLeafTag key;      /* Tag of a disk page */
  int    id;      /* Associated buffer id */
} PLeafLookupEnt;

static_assert(sizeof(PLeafLookupEnt) == (sizeof(PLeafTag) + sizeof(int)),
    "PLeaf Lookup Entry");

static hash_table_t *page_hash;

/*
 * PLeafHashInit
 *
 * Initialize pleaf hash in shared memory
 */
void PLeafHashInit()
{
  page_hash = ib_create(2 * NPLeafBuffers, LATCH_ID_HASH_TABLE_RW_LOCK,
      NPLeafPartitions, MEM_HEAP_FOR_PAGE_HASH);
  ut_a(page_hash != nullptr);
}

void PLeafHashFree()
{
  ha_clear(page_hash);
  hash_table_free(page_hash);
}
/*
 * PLeafHashCode
 *
 * Compute the hash code associated with a PLeafTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. 
 */
ulint PLeafHashCode(const PLeafTag *tagPtr)
{
  return (((ulint)tagPtr->page_id << 32) | tagPtr->gen_no);
}

/*
 * PLeafHashLookup
 *
 * Lookup the given PLeafTag; return pleaf page id, or -1 if not found
 * Caller must hold at least shared lock on PLeafHashLock for tag's
 * partition
 */
int PLeafHashLookup(const PLeafTag *tagPtr, ulint hashcode)
{
  const rec_t* result;

  result = ha_search_and_get_data(page_hash, hashcode);

  if (!result)
    return -1;
  return reinterpret_cast<int64_t>(
      reinterpret_cast<const int*>(result)) - 1;
}

/*
 * PLeafHashInsert
 *
 * Insert a hashtable entry for given tag and buffer id,
 * unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion. If a conflicting entry already exists,
 * returns its buffer ID.
 *
 * Caller must hold exclusive lock on PLeafHashLock for tag's partition
 */
void PLeafHashInsert(const PLeafTag *tagPtr, ulint hashcode, int buffer_id)
{
  ut_a(buffer_id >= 0);    /* -1 is reserved for not-in-table */

  buffer_id += 1;
  ha_insert_for_fold(page_hash, hashcode, nullptr, 
      reinterpret_cast<const rec_t*>(reinterpret_cast<int*>(buffer_id)));
}

/*
 * PLeafHashDelete
 *
 * Delete the hashtable entry for given tag (must exist)
 *
 * Caller must hold exclusive lock on PLeafHashLock for tag's partition
 */
void PLeafHashDelete(const PLeafTag *tagPtr, ulint hashcode, int buffer_id)
{
  bool result;

  ut_a(buffer_id >= 0);

  buffer_id += 1;
  result = ha_search_and_delete_if_found(page_hash, hashcode,
      reinterpret_cast<const rec_t*>(reinterpret_cast<int*>(buffer_id)));

  ut_a(result);
}

rw_lock_t* PLeafGetHashLock(ulint hashcode)
{
  return hash_get_lock(page_hash, hashcode);
}
#endif /* J3VM */

