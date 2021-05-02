#ifdef J3VM

#include "read0types.h"
#include "ha0ha.h"

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree.h"
#include "include/ebi_tree_buf.h"
#include "include/ebi_tree_hash.h"

#include <assert.h>

typedef struct {
  EbiTreeBufTag key; /* Tag of a disk page */
  int id;            /* Associated buffer id */
} EbiTreeLookupEnt;

static_assert(sizeof(EbiTreeLookupEnt) == (sizeof(EbiTreeBufTag) + sizeof(int)),
    "EbiTree Lookup Entry");

static hash_table_t *page_hash;

/*
 * EbiTreeHashInit
 *
 * Initialize ebi_tree hash in shared memory
 */
void EbiTreeHashInit() {
  page_hash = ib_create(2 * NEbiBuffers, LATCH_ID_HASH_TABLE_RW_LOCK,
      NEbiPartitions, MEM_HEAP_FOR_PAGE_HASH);
  ut_a(page_hash != nullptr);
}

void EbiTreeHashFree() {
  ha_clear(page_hash);
  hash_table_free(page_hash);
}
/*
 * EbiTreeHashCode
 *
 * Compute the hash code associated with a EbiTreeBufTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. We do this way because the callers need to know the hash code in
 * order to determine which buffer partition to lock, and we don't want to
 * do the hash computation twice (hash_any is a bit slow).
 */
ulint EbiTreeHashCode(const EbiTreeBufTag *tagPtr) {
  return (((ulint)tagPtr->page_id << 32) | tagPtr->seg_id);
  //return get_hash_value(SharedEbiTreeHash, (void *)tagPtr);
}

/*
 * EbiTreeHashLookup
 *
 * Lookup the given EbiTreeBufTag; return ebi_tree page id, or -1 if not found
 * Caller must hold at least shared lock on EbiTreeMappingLock for tag's
 * partition
 */
int EbiTreeHashLookup(const EbiTreeBufTag *tagPtr, ulint hashcode) {
  const rec_t* result;

  result = ha_search_and_get_data(page_hash, hashcode);

  if (!result) {
    return -1;
  } 

  return reinterpret_cast<int64_t>(
      reinterpret_cast<const int*>(result)) - 1;
}

/*
 * EbiTreeHashInsert
 *
 * Insert a hashtable entry for given tag and buffer id,
 * unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion. If a conflicting entry already exists,
 * returns its buffer ID.
 *
 * Caller must hold exclusive lock on EbiTreeMappingLock for tag's partition
 */
void EbiTreeHashInsert(const EbiTreeBufTag *tagPtr, 
    ulint hashcode,
    int buffer_id) {

  ut_a(buffer_id >= 0);    /* -1 is reserved for not-in-table */

  buffer_id += 1;
  ha_insert_for_fold(page_hash, hashcode, nullptr, 
      reinterpret_cast<const rec_t*>(reinterpret_cast<int*>(buffer_id)));
}

/*
 * EbiTreeHashDelete
 *
 * Delete the hashtable entry for given tag (must exist)
 *
 * Caller must hold exclusive lock on EbiTreeMappingLock for tag's partition
 */
void EbiTreeHashDelete(const EbiTreeBufTag *tagPtr, 
    ulint hashcode, 
    int buffer_id) {
  bool result;

  ut_a(buffer_id >= 0);

  buffer_id += 1;
  result = ha_search_and_delete_if_found(page_hash, hashcode,
      reinterpret_cast<const rec_t*>(reinterpret_cast<int*>(buffer_id)));

  ut_a(result);
}

rw_lock_t* EbiGetHashLock(ulint hashcode)
{
  return hash_get_lock(page_hash, hashcode);
}
#endif /* J3VM */
