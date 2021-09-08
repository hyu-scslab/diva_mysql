/*-------------------------------------------------------------------------
 *
 * ebi_tree_buf.c
 *
 * EBI Tree Buffer Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ebi_tree/ebi_tree_buf.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef J3VM

#include "read0types.h"
#include "trx0sys.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree_process.h"
#include "include/ebi_tree_buf.h"
#include "include/ebi_tree_hash.h"

EbiTreeBufDescPadded *EbiTreeBufDescriptors;
char *EbiTreeBufBlocks;
EbiTreeBufMeta *EbiTreeBuf;

extern GarbageQueue* gc_queue;

#define EBI_TREE_SEG_OFFSET_TO_PAGE_ID(off) ((off) / (EBI_TREE_SEG_PAGESZ))

/* Private functions */
static int EbiTreeBufGetBufRef(
    EbiTreeSegmentId seg_id,
    EbiTreeSegmentOffset seg_offset);

/* Segment control */
//static int OpenSegmentFile(EbiTreeSegmentId seg_id);
//static void CloseSegmentFile(int fd);
static void EbiTreeReadSegmentPage(const EbiTreeBufTag *tag, int buf_id);
static void EbiTreeWriteSegmentPage(const EbiTreeBufTag *tag, int buf_id);

static void EbiTreeBufUnrefInternal(EbiTreeBufDesc *buf);

void
EbiTreeBufInit(void) {
  EbiTreeBufDesc *buf;

  /* Align descriptors to a cacheline boundary */
  EbiTreeBufDescriptors = (EbiTreeBufDescPadded *) ut_zalloc_nokey(
      NEbiBuffers * sizeof(EbiTreeBufDescPadded));

  /* Buffer blocks */
  EbiTreeBufBlocks = (char *) ut_zalloc_nokey(
      NEbiBuffers * EBI_TREE_SEG_PAGESZ);

  EbiTreeHashInit();

  /* Initialize descriptors */
  for (int i = 0; i < NEbiBuffers; i++) {
    buf = GetEbiTreeBufDescriptor(i);
    buf->tag.seg_id = 0;
    buf->tag.page_id = 0;
    buf->is_dirty = false;
    buf->refcnt = 0;
  }

  EbiTreeBuf = (EbiTreeBufMeta *) ut_zalloc_nokey(sizeof(EbiTreeBufMeta));
}

void
EbiTreeBufFree(void) {
  EbiTreeHashFree();
  ut_free(EbiTreeBuf);
  ut_free(EbiTreeBufBlocks);
  ut_free(EbiTreeBufDescriptors);
}

void
EbiTreeAppendVersion(
    EbiTreeSegmentId seg_id,
    EbiTreeSegmentOffset seg_offset,
    ulint tuple_size,
    const void *tuple) {
  int buf_id;
  EbiTreeBufDesc *buf;
  int page_offset;

  buf_id = EbiTreeBufGetBufRef(seg_id, seg_offset);
  buf = GetEbiTreeBufDescriptor(buf_id);

  page_offset = seg_offset % EBI_TREE_SEG_PAGESZ;

  /* Copy the tuple into the cache */
  memcpy(
      &EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ + page_offset],
      tuple,
      tuple_size);

  ut_a(page_offset + tuple_size <= EBI_TREE_SEG_PAGESZ);

  /*
   * Mark it as dirty so that it could be flushed when evicted.
   */
  buf->is_dirty = true;

  /* Whether or not the page has been full, we should unref the page */
  EbiTreeBufUnrefInternal(buf);
}

int
EbiTreeReadVersionRef(
    EbiTreeSegmentId seg_id,
    EbiTreeSegmentOffset seg_offset,
    ulint tuple_size,
    rec_t **ret_value) {
  int buf_id;
  int page_offset;

  buf_id = EbiTreeBufGetBufRef(seg_id, seg_offset);

  page_offset = seg_offset % EBI_TREE_SEG_PAGESZ;

  /* Set ret_value to the pointer of the tuple in the cache */
  *ret_value = (rec_t*)&EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ + page_offset];

  /* The caller must unpin the buffer entry for buf_id */
  return buf_id;
}

/*
 * EbiTreeBufGetBufRef
 *
 * Increase refcount of the requested segment page, and returns the cache_id.
 * If the page is not cached, read it from the segment file. If cache is full,
 * evict one page following the eviction policy (currently round-robin..)
 * Caller must decrease refcount after using it. If caller makes the page full
 * by appending more tuple, it has to decrease one more count for unpinning it.
 */
static int
EbiTreeBufGetBufRef(EbiTreeSegmentId seg_id, EbiTreeSegmentOffset seg_offset) {
  EbiTreeBufTag tag;          /* identity of requested block */
  int buf_id;                 /* buf index of target segment page */
  int candidate_id;           /* buf index of victim segment page */
  rw_lock_t *new_partition_lock; /* buf partition lock for it */
  rw_lock_t *old_partition_lock; /* buf partition lock for it */
  ulint hashcode;
  ulint hashcode_vict;
  EbiTreeBufDesc *buf;
  int ret;
  EbiTreeBufTag victim_tag;

  tag.seg_id = seg_id;
  tag.page_id = EBI_TREE_SEG_OFFSET_TO_PAGE_ID(seg_offset);

  /* Get hash code for the segment id & page id */
  hashcode = EbiTreeHashCode(&tag);
  new_partition_lock = EbiGetHashLock(hashcode);

  rw_lock_s_lock(new_partition_lock);

  buf_id = EbiTreeHashLookup(&tag, hashcode);
  if (buf_id >= 0) {
    /* Target page is already in cache */
    buf = GetEbiTreeBufDescriptor(buf_id);

    /* Increase refcount by 1, so this page couldn't be evicted */
    __sync_fetch_and_add(&buf->refcnt, 1);
    rw_lock_s_unlock(new_partition_lock);

    return buf_id;
  }

  /*
   * Need to acquire exclusive lock for inserting a new vcache_hash entry
   */
  rw_lock_s_unlock(new_partition_lock);
  rw_lock_x_lock(new_partition_lock);

  buf_id = EbiTreeHashLookup(&tag, hashcode);
  if (buf_id >= 0) {
    buf = GetEbiTreeBufDescriptor(buf_id);

    __sync_fetch_and_add(&buf->refcnt, 1);
    rw_lock_x_unlock(new_partition_lock);

    return buf_id;
  }

find_cand:
  /* Pick up a candidate cache entry for a new allocation */
  candidate_id = __sync_fetch_and_add(&EbiTreeBuf->eviction_rr_idx, 1) %
                 NEbiBuffers;
  buf = GetEbiTreeBufDescriptor(candidate_id);
  if (buf->refcnt != 0) {
    /* Someone is accessing this entry, find another candidate */
    goto find_cand;
  }
  victim_tag = buf->tag;

  /*
   * It seems that this entry is unused now. But we need to check it
   * again after holding the partition lock, because another transaction
   * might trying to access and increase this refcount just right now.
   */
  if (victim_tag.seg_id > 0) {
    /*
     * This entry is using now so that we need to remove vcache_hash
     * entry for it. We also need to flush it if the cache entry is dirty.
     */
    hashcode_vict = EbiTreeHashCode(&buf->tag);
    old_partition_lock = EbiGetHashLock(hashcode_vict);

    if (!rw_lock_x_lock_nowait(old_partition_lock)) {
      /* Partition lock is already held by someone. */
      goto find_cand;
    }

    /* Try to hold refcount for the eviction */
    ret = __sync_fetch_and_add(&buf->refcnt, 1);
    if (ret > 0) {
      /*
       * Race occured. Another read transaction might get this page,
       * or possibly another evicting tranasaction might get this page
       * if round robin cycle is too short.
       */
      __sync_fetch_and_sub(&buf->refcnt, 1);
      rw_lock_x_unlock(old_partition_lock);
      goto find_cand;
    }

    if (buf->tag.seg_id != victim_tag.seg_id ||
        buf->tag.page_id != victim_tag.page_id) {
      /*
       * This exception might very rare, but the possible scenario is,
       * 1. txn A processed up to just before holding the
       *    old_partition_lock
       * 2. round robin cycle is too short, so txn B acquired the
       *    old_partition_lock, and evicted this page, and mapped it
       *    to another vcache_hash entry
       * 3. Txn B unreffed this page after using it so that refcount
       *    becomes 0, but seg_id and(or) page_id of this entry have
       *    changed
       * In this case, just find another victim for simplicity now.
       */
			__sync_fetch_and_sub(&buf->refcnt, 1);
      rw_lock_x_unlock(old_partition_lock);
      goto find_cand;
    }

    /*
     * Now we are ready to use this entry as a new cache.
     * First, check whether this victim should be flushed to segment file.
     * Appending page shouldn't be picked as a victim because of the refcount.
     */
    if (buf->is_dirty) {
      /* There is consensus protocol between evict page and cut segment. */
      /* The point is that never evict(flush) page which is
       * included in the file of cutted-segment */
			ut_a(buf->tag.seg_id != 0);

      /* Check if the page related segment file has been removed */
			// [JS_URGENT]
			uint64_t my_slot = gc_queue->increase_ref_count();
      if (EbiTreeSegIsAlive(EbiTreePtr->ebitree, buf->tag.seg_id)) {
        EbiTreeWriteSegmentPage(&buf->tag, candidate_id);
      } 
			gc_queue->decrease_ref_count(my_slot);

      /*
       * We do not zero the page so that the page could be overwritten
       * with a new tuple as a new segment page.
       */
      buf->is_dirty = false;
    }

    /*
     * Now we can safely evict this entry.
     * Remove corresponding hash entry for it so that we can release
     * the partition lock.
     */
    EbiTreeHashDelete(&buf->tag, hashcode_vict, candidate_id);
    rw_lock_x_unlock(old_partition_lock);
  } else {
    /*
     * This cache entry is unused. Just increase the refcount and use it.
     */
    ret = __sync_fetch_and_add(&buf->refcnt, 1);
    if (ret > 0 || buf->tag.seg_id != 0) {
      /*
       * Race occured. Possibly another evicting tranasaction might get
       * this page if round robin cycle is too short.
       */
      __sync_fetch_and_sub(&buf->refcnt, 1);
      goto find_cand;
     }  
  }

  /* Initialize the descriptor for a new cache */
  buf->tag = tag;

  /* Read target segment page into the cache */
  EbiTreeReadSegmentPage(&buf->tag, candidate_id);

  /* Next, insert new vcache hash entry for it */
  EbiTreeHashInsert(&tag, hashcode, candidate_id);

  rw_lock_x_unlock(new_partition_lock);

  /* Return the index of cache entry, holding refcount 1 */
  return candidate_id;
}

/* Segment open & close */

/*
 * EbiTreeCreateSegmentFile
 *
 * Make a new file for corresponding seg_id
 */
void
EbiTreeCreateSegmentFile(EbiTreeSegmentId seg_id) {
  int fd;
  char filename[128];

  sprintf(filename, "ebitree.%09d", seg_id);
  fd = open(filename, O_RDWR | O_CREAT, (mode_t)0600);

  ut_a(fd >= 0);

  close(fd);
}

/*
 * EbiTreeRemoveSegmentFile
 *
 * Remove file for corresponding seg_id
 */
void
EbiTreeRemoveSegmentFile(EbiTreeSegmentId seg_id) {
  char filename[128];

  sprintf(filename, "ebitree.%09d", seg_id);

  ut_a(remove(filename) == 0);
}

/*
 * OpenSegmentFile
 *
 * Open Segment file.
 * Caller have to call CloseSegmentFile(seg_id) after file io is done.
 */
int
OpenSegmentFile(EbiTreeSegmentId seg_id) {
  int fd;
  char filename[128];

  sprintf(filename, "ebitree.%09d", seg_id);
  fd = open(filename, O_RDWR, (mode_t)0600);

  ut_a(fd >= 0);

  return fd;
}

/*
 * CloseSegmentFile
 *
 * Close Segment file.
 */
void
CloseSegmentFile(int fd) {
  ut_a(fd >= 0);

  close(fd);
}

static void
EbiTreeReadSegmentPage(const EbiTreeBufTag *tag, int buf_id) {
  ssize_t read;
  int fd;

  fd = OpenSegmentFile(tag->seg_id);

  ut_a(fd >= 0);

  read = pread(
      fd, &EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ],
      EBI_TREE_SEG_PAGESZ, (off_t)tag->page_id * EBI_TREE_SEG_PAGESZ);

  /* New page */
  /* TODO: not a clean logic */
  if (read == 0) {
    memset(
        &EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ],
        0x00,
        EBI_TREE_SEG_PAGESZ);
  }

  CloseSegmentFile(fd);
}

static void
EbiTreeWriteSegmentPage(const EbiTreeBufTag *tag, int buf_id) {
  int fd;

  fd = OpenSegmentFile(tag->seg_id);

  ut_a(
      EBI_TREE_SEG_PAGESZ ==
      pwrite(
          fd, &EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ],
          EBI_TREE_SEG_PAGESZ, (off_t)tag->page_id * EBI_TREE_SEG_PAGESZ));

  CloseSegmentFile(fd);
}

/*
 * EbiTreeBufUnref
 *
 * A public version of EbiTreeBufUnrefInternal
 */
void
EbiTreeBufUnref(int buf_id) {
  EbiTreeBufDesc *buf;
  ut_a(EbiTreeBufIsValid(buf_id));

  buf = GetEbiTreeBufDescriptor(buf_id);
  EbiTreeBufUnrefInternal(buf);
}

bool
EbiTreeBufIsValid(int buf_id) {
  return buf_id < NEbiBuffers;
}

/*
 * EbiTreeBufUnrefInternal
 *
 * Decrease the refcount of the given buf entry
 */
static void
EbiTreeBufUnrefInternal(EbiTreeBufDesc *buf) {
  ut_a(buf->refcnt != 0);
  __sync_fetch_and_sub(&buf->refcnt, 1);
}

#endif /* J3VM */
