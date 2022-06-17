/*-------------------------------------------------------------------------
 *
 * pleaf_hash.cc
 * 		Hash table implementation for mapping PLeafPage to PLeafBuffer indexes. 
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
#endif /* DIVA */

