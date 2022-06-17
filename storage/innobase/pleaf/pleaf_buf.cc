/*-------------------------------------------------------------------------
 *
 * pleaf_buf.cc
 * 		PLeaf buffer implementation. 
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
#include "trx0sys.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "include/pleaf.h"
#include "include/pleaf_stack_helper.h"
#include "include/pleaf_bufpage.h"
#include "include/pleaf_buf.h"
#include "include/pleaf_hash.h"

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree_process.h"

/* Version Data files and Info file descriptors */
int versioninfo_fd;
int versiondata_fd[NPLeafPools];

/* rwlocks */
rw_lock_t* pleaf_rwlocks;

/* Shared memory objects */
PLeafDescPadded  *PLeafDescriptors;
char      *PLeafBlocks;
PLeafMetaPadded  *PLeafMetadata;
PLeafFreeManager  PLeafManager;

/* Decls of static functions */
static int PLeafGetPageInternal(PLeafPageId page_id, 
                          PLeafGenNumber gen_no,
                          bool is_new);

static void PLeafReleasePageInternal(PLeafDesc *frame);

static void PLeafReadPage(const PLeafTag *tag, 
                          int frame_id);
static void PLeafWritePage(const PLeafTag *tag, 
                          int frame_id);

static void PLeafInitDataFile(int pool_index);
static PLeafGenNumber PLeafInitInfoFile(void);
static PLeafGenNumber PLeafUpdateVersionInfo(void);

static PLeafGenNumber PLeafInitFiles(void);
static void PLeafFreeFiles(void);

static void PLeafInitMeta(PLeafGenNumber gen_no);

static void PLeafInitStack(void);
static PLeafFreeStack PLeafGetFreeStack(PLeafFreePool free_pool,
                          int cap_index,
                          int inst_no);
static void PLeafStackPush(PLeafFreeStack free_stack, 
                          PLeafPage page, 
                          PLeafPageId page_id) ;

static PLeafPage PLeafStackPop(PLeafFreeStack free_stack, 
                          int *frame_id,
                          PLeafGenNumber gen_no,
                          int cap_index, 
                          int inst_no); 

static int PLeafGetPoolIndex(PLeafGenNumber gen_no);

static void PLeafCleanOldStacks(int pool_index);
static void PLeafCleanOldFile(int pool_index);


/*
 * Shared memory initialization
 */
void
PLeafInit(void)
{
  PLeafDesc  *frame;

  int      i;
  uint32  gen_no;

  /* Align descriptors to a cacheline boundary. */
  PLeafDescriptors = (PLeafDescPadded *) ut_zalloc_nokey (
      NPLeafBuffers * sizeof(PLeafDescPadded));

  /* Allocate Buffer Blocks */
  PLeafBlocks = (char *) ut_zalloc_nokey(
      NPLeafBuffers * PLEAF_PAGE_SIZE);

  /* Allocate and initialize the shared pleaf hashtable */
  PLeafHashInit();

  /* Allocate pleaf metadata */
  PLeafMetadata = (PLeafMetaPadded *) ut_zalloc_nokey(
      sizeof(PLeafMetaPadded));
  
  /* Allocate pleaf manager */
  PLeafManager = (PLeafFreeManager) ut_zalloc_nokey(
      sizeof(PLeafFreeManagerData));

  /* Allocate pleaf pool instances */
  for (i = 0; i < NPLeafPools; ++i)
    PLeafManager->pools[i].free_instances = 
      (PLeafFreeInstance) ut_zalloc_nokey(
          NPLeafInstances * sizeof(PLeafFreeInstanceData));

  /* Initialize pleaf descriptors */
  for (i = 0; i < NPLeafBuffers; i++)
  {
    frame = GetPLeafDescriptor(i);
    CLEAR_PLEAFTAG(frame->tag);
    frame->is_dirty = false;
    frame->refcount = 0;
  }

  /* Allocate and create pleaf rwlocks */
  pleaf_rwlocks = static_cast<rw_lock_t *>(
      ut_zalloc_nokey(sizeof(*pleaf_rwlocks) * NPLeafPools));
  
  for (i = 0; i < NPLeafPools; ++i)
    rw_lock_create(pleaf_lock_key, &pleaf_rwlocks[i], SYNC_PLEAF);

  /* Initialize pleaf files */
  gen_no = PLeafInitFiles();

  /* Initialize pleaf metadata */
  PLeafInitMeta(gen_no);

  /* Initialize pleaf manager */
  PLeafInitStack();
}

void
PLeafFree(void)
{
  int i;

  PLeafFreeFiles();

  for (i = 0; i < NPLeafPools; ++i)
    rw_lock_free(&pleaf_rwlocks[i]);

  ut_free(pleaf_rwlocks);

  for (i = 0; i < NPLeafPools; ++i)
    ut_free(PLeafManager->pools[i].free_instances);

  ut_free(PLeafManager);
  ut_free(PLeafMetadata);

  PLeafHashFree();

  ut_free(PLeafBlocks);
  ut_free(PLeafDescriptors);
}

static void
PLeafInitMeta(PLeafGenNumber gen_no)
{
  PLeafMeta *meta;

  meta = &PLeafMetadata->pleafmeta;
  meta->eviction_rr_idx = 0;
  meta->max_page_ids[LEFT_POOL] = NPLeafInitPages * NPLeafInstances;
  meta->max_page_ids[RIGHT_POOL] = 0;
  meta->recent_index = LEFT_POOL;
  meta->generation_max_xid = 0;
  meta->generation_seq_no = 0;
  
  memset(meta->generation_numbers, 0x00, sizeof(PLeafGenNumber) * NPLeafPools);
  meta->generation_numbers[meta->recent_index] = gen_no;
}

static PLeafGenNumber PLeafInitInfoFile(void)
{
  PLeafGenNumber ret_gen_no;
  char filename[64];
  size_t gen_size = sizeof(PLeafGenNumber);

  sprintf(filename, "pleafversion.info");

  versioninfo_fd = open(filename, O_RDWR | O_CREAT, (mode_t)0600);
  ut_a(versioninfo_fd >= 0);

  if (pread(versioninfo_fd, &ret_gen_no, 
                                gen_size, 0) != int(gen_size))
  {
    /* File is created now */
    ret_gen_no = 1;

    ut_a(pwrite(
          versioninfo_fd, &ret_gen_no, gen_size, 0) == int(gen_size));
  }
  else
  {
    /* We use 2-bytes for generation number */
    ret_gen_no += 1;
    ut_a(ret_gen_no != InvalidGid);
    ut_a(pwrite(
          versioninfo_fd, &ret_gen_no, 
                          gen_size, 0) == int(gen_size));
  }
  return ret_gen_no;
}

static PLeafGenNumber
PLeafUpdateVersionInfo(void)
{

  PLeafGenNumber ret_gen_no;
  size_t gen_size = sizeof(PLeafGenNumber);

  ut_a(versioninfo_fd >= 0);

  if (pread(versioninfo_fd, &ret_gen_no, 
                                    gen_size, 0) != int(gen_size))
  {
    ut_a(false);
  }
  else
  {
    /* We use 2-bytes for generation number */
    ret_gen_no += 1;
    ut_a(ret_gen_no != InvalidGid);

    ut_a(pwrite(versioninfo_fd, &ret_gen_no, 
                              gen_size, 0) == int(gen_size));
  }
  return ret_gen_no;
}

static void
PLeafInitDataFile(int pool_index)
{
  
  if (versiondata_fd[pool_index] < 0)
  {
    char filename[64];
    sprintf(filename, "pleafversion.data%d", pool_index);

    versiondata_fd[pool_index] = open(filename, O_RDWR | O_CREAT, (mode_t)0600);
    ut_a(versiondata_fd[pool_index] >= 0);
    ut_a(fallocate(
          versiondata_fd[pool_index], 0, 0, PLEAF_INIT_FILE_SIZE) == 0);
    ut_a(ftruncate(versiondata_fd[pool_index], PLEAF_INIT_FILE_SIZE) == 0);  
  }
  else
  {
    ut_a(ftruncate(versiondata_fd[pool_index], PLEAF_INIT_FILE_SIZE) == 0);  
  }
}

static PLeafGenNumber
PLeafInitFiles(void)
{
  int i;
  PLeafGenNumber ret_gen_no;

  ret_gen_no = PLeafInitInfoFile();

  for (i = 0; i < NPLeafPools; ++i) {
    versiondata_fd[i] = -1;
    PLeafInitDataFile(i);
  }

  return ret_gen_no;
}

static void
PLeafFreeFiles(void)
{
  int i;

  if (versioninfo_fd >= 0)
    close(versioninfo_fd);

  for (i = 0; i < NPLeafPools; ++i)
    if (versiondata_fd[i] >= 0)
      close(versiondata_fd[i]);
}

static int
PLeafGetPageInternal(PLeafPageId page_id, 
              PLeafGenNumber gen_no,
              bool is_new)
{
  PLeafTag    pleaf_tag, victim_tag;
  int    frame_id, candidate_id;
  rw_lock_t    *new_partition_lock;
  rw_lock_t    *old_partition_lock;
  ulint    hashcode, hashcode_vict;
  PLeafDesc  *frame;
  int    ret;

  /* Set page id and generation number for hashing */
  CLEAR_PLEAFTAG(pleaf_tag);
  INIT_PLEAF_TAG(pleaf_tag, page_id, gen_no);

  /* Get hashcode based on page id and generation number */
  hashcode = PLeafHashCode(&pleaf_tag);
  new_partition_lock = PLeafGetHashLock(hashcode);

  rw_lock_s_lock(new_partition_lock);
  frame_id = PLeafHashLookup(&pleaf_tag, hashcode);
  
  if (frame_id >= 0)
  {
    /* Target page is already in buffer */
    frame = GetPLeafDescriptor(frame_id);

    /* Increase refcount by 1, so this page shouldn't be evicted */
    __sync_fetch_and_add(&frame->refcount, 1);

    rw_lock_s_unlock(new_partition_lock);

    return frame_id;
  }

  /* Need to acquire exclusive lock for inserting a new hash entry */
  rw_lock_s_unlock(new_partition_lock);
  rw_lock_x_lock(new_partition_lock);

  /* Another transaction could insert hash entry */
  frame_id = PLeafHashLookup(&pleaf_tag, hashcode);

  if (frame_id >= 0)
  {
    frame = GetPLeafDescriptor(frame_id);

    __sync_fetch_and_add(&frame->refcount, 1);

    rw_lock_x_unlock(new_partition_lock);

    return frame_id;
  }

find_cand:
  /* Pick up a candidate entry for a new allocation */
  candidate_id = __sync_fetch_and_add(
      &PLeafMetadata->pleafmeta.eviction_rr_idx, 1) % NPLeafBuffers;

  frame = GetPLeafDescriptor(candidate_id);

  if (frame->refcount != 0)
  {
    /* Someone is accessing this entry simultaneously, find another one */
    goto find_cand;
  }

  victim_tag = frame->tag;

  if (victim_tag.gen_no > 0)
  {
    /*
     * This entry is using now so that we need to remove hash entry for it.
     * We also need to flush it if the entry is dirty 
     */
    hashcode_vict = PLeafHashCode(&frame->tag);
    old_partition_lock = PLeafGetHashLock(hashcode_vict);

    if (!rw_lock_x_lock_nowait(old_partition_lock))
    {
      /* Partition lock is already held by other */
      goto find_cand;
    }

    /* Try to hold refcount for the eviction */
    ret = __sync_fetch_and_add(&frame->refcount, 1);
    
    if (ret > 0)
    {
      /*
       * Race occured. Another read transaction might get this page,
       * or possibly another evicting transaction might get this page
       * if round robin cycle is too short
       */
      __sync_fetch_and_sub(&frame->refcount, 1);
      rw_lock_x_unlock(old_partition_lock);
      goto find_cand;
    }

    if (frame->tag.page_id != victim_tag.page_id ||
        frame->tag.gen_no != victim_tag.gen_no)
    {
      /*
       * This exception might very rare, but the possible scenario is,
       * 1. txn A processed up to just before holding the
       * old_partition_lock
       * 2. round robin cycle is too short, so txn B acquired the
       * old_partition_lock, and evicted this page, and mapped it to another 
       * hash entry
       * 3. txn B unref this page after using it so that refcount
       * becomes 0, but page_id and(or) gen_no of this entry have changed
       * In this case, just find another victim for simplicity now
       */
      rw_lock_x_unlock(old_partition_lock);
      goto find_cand;
    }

    if (frame->is_dirty)
    {
      /*
       * JAESEON: GENERATION NUMBER
       * Write dirty page to disk will be not executed depends on
       * generation number (active or inactive).
       */

      PLeafWritePage(&frame->tag, candidate_id);
      frame->is_dirty = false;
    }

    /*
     * Now we can safely evict this entry.
     * Remove corresponding hash entry for it so that we can release
     * the partition lock
     */
    PLeafHashDelete(&frame->tag, hashcode_vict, candidate_id);
    rw_lock_x_unlock(old_partition_lock);
  }
  else
  {
    /*
     * This entry is unused. Increase the refcount and use it
     */
    ut_a(!frame->is_dirty);
  
    ret = __sync_fetch_and_add(&frame->refcount, 1);
    if (ret > 0)
    {
      /*
       * Race occured. Possibly another evicting transaction might
       * get this page if round robin cycle is too short.
       */
      __sync_fetch_and_sub(&frame->refcount, 1);
      goto find_cand;
    }
  }

  frame->tag = pleaf_tag;

  PLeafHashInsert(&pleaf_tag, hashcode, candidate_id);

  /*
   * If requested page is not in the disk yet, we don't need to
   * read it from the disk, and it's actually not in the disk.
   * NOTE: Initialization will be done by caller's routine,
   * because it needs page's capacity and instance number
   * There are two cases is_new is true. 
   * See PLeafInit() & PLeafStackPop().
   */
  if (!is_new)
    PLeafReadPage(&frame->tag, candidate_id);

  rw_lock_x_unlock(new_partition_lock);

  /* Return the index of cache entry, holding refcount 1 */
  return candidate_id;
}

/* PLeafReadPage */
static void
PLeafReadPage(const PLeafTag *tag, 
          int frame_id)
{
  int pool_index;
  
  pool_index = PLeafGetPoolIndex(tag->gen_no);
  ut_a(pool_index != -1);
  ut_a(versiondata_fd[pool_index] >= 0);

  ut_a(PLEAF_PAGE_SIZE == 
      pread(versiondata_fd[pool_index],
        &PLeafBlocks[frame_id * PLEAF_PAGE_SIZE],
                  PLEAF_PAGE_SIZE, (off_t)tag->page_id * PLEAF_PAGE_SIZE));
}

/* PLeafWritePage */
static void
PLeafWritePage(const PLeafTag *tag, 
          int frame_id)
{
  int current_gen_no, pool_index;
  /* Current pleaf generation number is already expired. */
  pool_index = PLeafGetPoolIndex(tag->gen_no);

  if (pool_index == -1)
    return;

  rw_lock_s_lock(&pleaf_rwlocks[pool_index]);

  current_gen_no = PLeafMetadata->pleafmeta.generation_numbers[pool_index];

  if (current_gen_no != tag->gen_no)
  {
    /* Current pleaf generation number is expired now */
    ut_a(current_gen_no == 0 || current_gen_no > tag->gen_no + 1);

    rw_lock_s_unlock(&pleaf_rwlocks[pool_index]);
    return;
  }

  ut_a(versiondata_fd[pool_index] >= 0);

  ut_a(PLEAF_PAGE_SIZE == 
      pwrite(versiondata_fd[pool_index], 
        &PLeafBlocks[frame_id * PLEAF_PAGE_SIZE],
                  PLEAF_PAGE_SIZE, (off_t)tag->page_id * PLEAF_PAGE_SIZE));

  rw_lock_s_unlock(&pleaf_rwlocks[pool_index]);
}

void
PLeafMarkDirtyPage(int frame_id)
{
  PLeafDesc* frame;

  frame = GetPLeafDescriptor(frame_id);
  frame->is_dirty = true;
}

/*
 * PLeafGetPage
 *
 * Interface for requesting a page with page id and generation number.
 * P-leaf buffer's hashcode consists of page id and generation number.
 *
 * When we request a page not in disk yet, the value of is_new is true.
 */
PLeafPage
PLeafGetPage(PLeafPageId page_id,
      PLeafGenNumber gen_no,
      bool is_new, 
      int *frame_id) 
{
  *frame_id = PLeafGetPageInternal(page_id, gen_no, is_new);
  ut_a(*frame_id < NPLeafBuffers);
  return (PLeafPage) &PLeafBlocks[*frame_id * PLEAF_PAGE_SIZE];
}


/*
 * PLeafGetFreePage
 *
 * Interface for requesting a free page with the smallest capacity
 * and random instance number from the given free pool.
 */
PLeafPage
PLeafGetFreePage(int *frame_id,
          PLeafFreePool free_pool) 
{
  uint64_t inst_no;
  PLeafFreeStack free_stack;
  PLeafPage page;

  inst_no = __sync_fetch_and_add(
        &free_pool->rr_counter, 1) % NPLeafInstances;
  
  free_stack = PLeafGetFreeStack(free_pool, 0, inst_no);
  page = PLeafStackPop(free_stack, frame_id, free_pool->gen_no, 0, inst_no);

  ut_a(page != nullptr);
  return page;
}

PLeafPage
PLeafGetFreePageWithCapacity(int *frame_id, 
            PLeafFreePool free_pool,
            int cap_index, 
            int inst_no) 
{
  PLeafFreeStack free_stack;
  PLeafPage page;

  free_stack = PLeafGetFreeStack(free_pool, cap_index, inst_no);
  page = PLeafStackPop(free_stack, frame_id, 
                free_pool->gen_no, cap_index, inst_no);

  ut_a(cap_index <= N_PAGE_CAP_ARR); 
  ut_a(page != nullptr);
  return page;
}

/*
 * PLeafFindFreeSlot
 *
 * Find the free slot(internal bitamp), and then return its offset value
 */
PLeafOffset
PLeafFindFreeSlot(PLeafPage page, 
          int frame_id, 
          PLeafFreePool free_pool,
          uint32_t type) 
{
  PLeafOffset offset;
  PLeafPageId page_id;
  PLeafVersionIndex version_index;
  PLeafFreeStack free_stack;
  PLeafDesc *frame;
  bool is_full;
  int capacity, array_index;

  frame = GetPLeafDescriptor(frame_id);
  page_id = frame->tag.page_id;

  is_full = PLeafPageSetBitmap(page, page_id, &offset);

  capacity = PLeafPageGetCapacity(page);

  array_index = PLeafPageGetArrayIndex(capacity, offset);
  
  ut_a(array_index >= 0);

  version_index = PLeafPageGetVersionIndex(page, array_index);

  /* Initialization */
  *version_index = type;

  PLeafMarkDirtyPage(frame_id);
  if (!is_full) {
    /* If a page has free space, then push it to stack */
    free_stack = PLeafGetFreeStack(free_pool,
        PLeafPageGetCapacityIndex(page), PLeafPageGetInstNo(page));

    PLeafStackPush(free_stack, page, page_id);
  }

  ut_a(PLEAF_OFFSET_TO_GEN_NUMBER(offset) == 0);
  return offset;
}

/*
 * PLeafReleaseFreeSlot
 * Release its slot allocated before.
 */
void
PLeafReleaseFreeSlot(PLeafPage page, 
          int frame_id, 
          PLeafFreePool free_pool,
          int capacity, 
          int array_index) 
{
  PLeafPageId page_id;
  PLeafDesc *frame;
  PLeafVersionIndex version_index;
  PLeafFreeStack free_stack;
  bool was_full;

  frame = GetPLeafDescriptor(frame_id);
  page_id = frame->tag.page_id;

  ut_a(array_index >= 0);
  version_index = PLeafPageGetVersionIndex(page, array_index);
  
  *version_index = 0;

  was_full = PLeafPageUnsetBitmap(page, array_index);
  
  PLeafMarkDirtyPage(frame_id);
  if (was_full) {
    free_stack = PLeafGetFreeStack(free_pool,
        PLeafPageGetCapacityIndex(page), PLeafPageGetInstNo(page));

    PLeafStackPush(free_stack, page, page_id);
  }
}

/*
 * PLeafReleasePage
 */
void
PLeafReleasePage(int frame_id) 
{
  PLeafDesc *frame;

  ut_a(frame_id < NPLeafBuffers);

  frame = GetPLeafDescriptor(frame_id);
  PLeafReleasePageInternal(frame);
}

/*
 * PLeafReleasePageInternal
 * Decrease reference count
 */
static void
PLeafReleasePageInternal(PLeafDesc *frame)
{
  ut_a(frame->refcount != 0);
  __sync_fetch_and_sub(&frame->refcount, 1);
}

/*
 * PLeafIsOffsetValid
 * called by update transactions only
 */
bool
PLeafIsOffsetValid(PLeafOffset offset)
{
  PLeafMeta* meta = &PLeafMetadata->pleafmeta;
  PLeafGenNumber cur_left = meta->generation_numbers[LEFT_POOL];
  PLeafGenNumber cur_right = meta->generation_numbers[RIGHT_POOL];

  PLeafGenNumber cur_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(offset);

  /*
   * We thought a generation number in offset(param) was the recent one.
   * Recent generation number could be changed at this moment, but it is
   * safe to append new version to old generation version array.
   * Because it is guaranteed that the generation number accessed by this 
   * transaction (i.e. current generation number or recent generation number) 
   * cannot be cleaned before it committed.
   */
  return (((cur_gen_no == cur_left) || (cur_gen_no == cur_right)) &&
              (offset != PLEAF_INVALID_OFFSET));
}

/*
 * PLeafIsGenerationNumberValid
 * called by update transactions only
 */
bool
PLeafIsGenerationNumberValidInUpdate(PLeafGenNumber left, 
                    PLeafGenNumber right)
{
  PLeafMeta* meta = &PLeafMetadata->pleafmeta;
  PLeafGenNumber cur_left = meta->generation_numbers[LEFT_POOL];
  PLeafGenNumber cur_right = meta->generation_numbers[RIGHT_POOL];

  /*
   * It is an error that both generation numbers are active in here.
   */
  ut_a(!((left != 0 && (left == cur_left || left == cur_right)) &&
            (right != 0 && (right == cur_left || right == cur_right))));
  return ((left != 0 && (left == cur_left || left == cur_right)) ||
              (right != 0 && (right == cur_left || right == cur_right)));
}

/*
 * PLeafIsGenerationNumberValidInLookup
 * called by read transactions only
 */
void
PLeafIsGenerationNumberValidInLookup(PLeafGenNumber gen_no) 
{
  PLeafMeta* meta = &PLeafMetadata->pleafmeta;
  PLeafGenNumber cur_left = meta->generation_numbers[LEFT_POOL];
  PLeafGenNumber cur_right = meta->generation_numbers[RIGHT_POOL];

  ut_a(gen_no != 0);
  ut_a((gen_no == cur_left) || (gen_no == cur_right));
}

/*
 * PLeafGetOldGenerationNumber
 * called by read transactions only
 */
PLeafGenNumber
PLeafGetOldGenerationNumber(void)
{
  PLeafMeta* meta = &PLeafMetadata->pleafmeta;
  PLeafGenNumber cur_left = meta->generation_numbers[LEFT_POOL];
  PLeafGenNumber cur_right = meta->generation_numbers[RIGHT_POOL];

  ut_a(!(cur_left == 0 && cur_right == 0));
  
  if (cur_left == 0)
    return cur_right;
  else if (cur_right == 0)
    return cur_left;
  else if (cur_left < cur_right)
    return cur_left;
  else
    return cur_right;
}

PLeafPageId
PLeafFrameToPageId(int frame_id)
{
  PLeafDesc *frame;
  ut_a(frame_id >= 0);
  frame = GetPLeafDescriptor(frame_id);
  return frame->tag.page_id;
}

/* STACK */
static void
PLeafInitPool(PLeafFreePool free_pool)
{
  PLeafFreeStack stack;
  int i, j;
  for (i = 0; i < NPLeafInstances; ++i)
  {
    for (j = 0; j < NUM_OF_STACKS; ++j)
    {
      stack = PLeafGetFreeStack(free_pool, j, i);
      stack->head = PLEAF_STACK_HEAD_INIT;
      memset(&stack->elim_array, 0x00, sizeof(EliminationArrayData));
    }
  }
}

static void
PLeafInitStack(void)
{
  int i, j;
  PLeafFreePool free_pool;
  PLeafFreeStack stack;
  PLeafGenNumber gen_no;
  PLeafPage page;
  int frame_id;

  /* In initialize phase, it is safe to access first array entry */
  gen_no = PLeafMetadata->pleafmeta.generation_numbers[LEFT_POOL];

  /* Initialize both stacks */
  for (i = 0; i < NPLeafPools; ++i)
  {
    free_pool = &PLeafManager->pools[i];
    free_pool->rr_counter = 0;
    
    if (i == LEFT_POOL)
      free_pool->gen_no = gen_no;
    else
      free_pool->gen_no = 0;

    PLeafInitPool(free_pool);
  }

  /*
   * JS: For now, all pages in initialization will be in the first stack that
   * has the smallest capacity
   */
  free_pool = &PLeafManager->pools[LEFT_POOL];

  for (i = 0; i < NPLeafInstances; ++i)
  {
    for (j = 0; j < NPLeafInitPages; ++j)
    {
      stack = PLeafGetFreeStack(free_pool, 0, i);

      page = PLeafGetPage(i * NPLeafInitPages + j, gen_no, true, &frame_id);

      PLeafPageSetCapAndInstNo(page, 0, i);
      PLeafPageInitBitmap(page, 0);

      PLeafStackPush(stack, page, PLeafFrameToPageId(frame_id));

      PLeafMarkDirtyPage(frame_id);
      PLeafReleasePage(frame_id);
    }
  }
}

/*
 * PLeafGetFreeStack
 *
 * Found the stack with capacity index and instance number,
 * and return its pointer
 */
static PLeafFreeStack
PLeafGetFreeStack(PLeafFreePool free_pool,
            int cap_index,
            int inst_no)
{
  return &free_pool->free_instances[inst_no].free_stacks[cap_index];
}

/* PLeafStackTryPush */
static bool 
PLeafStackTryPush(PLeafFreeStack free_stack, 
            PLeafPage page, 
            PLeafPageId page_id) 
{
  PLeafPageMetadata head, new_head;
  PLeafStackTimestamp head_timestamp;
  PLeafPageId head_page_id;

  head = free_stack->head;
  head_timestamp = PLeafStackGetTimestamp(head);
  head_page_id = PLeafStackGetPageId(head);
  new_head = PLeafStackMakeHead(head_timestamp, page_id);

  PLeafPageSetNextPageId(page, head_page_id);

  return __sync_bool_compare_and_swap(&free_stack->head, head, new_head);
}

/* PLeafStackPush */
static void 
PLeafStackPush(PLeafFreeStack free_stack, 
          PLeafPage page, 
          PLeafPageId page_id) 
{
  while (true) {
    if (PLeafStackTryPush(free_stack, page, page_id))
      break;
  }
}

/* PLeafStackTryPop */
static PLeafPage 
PLeafStackTryPop(PLeafFreeStack free_stack, 
            PLeafGenNumber gen_no,
            int *frame_id) 
{
  PLeafPage head_page;
  int head_frame_id;
  PLeafPageId next_page_id, head_page_id;
  PLeafStackTimestamp head_timestamp;
  PLeafPageMetadata head, new_head;

  /* Get page id and timestamp from stack head */
  head = free_stack->head;
  head_page_id = PLeafStackGetPageId(head);
  head_timestamp = PLeafStackGetTimestamp(head);

  ut_a(head_timestamp < 0xFFFFFFFF);
  /* Empty stack */
  if (head_page_id == PLEAF_INVALID_PAGE_ID) {
    return nullptr;
  }

  /* Get head page and next page id from head page */
  head_page = PLeafGetPage(head_page_id, gen_no, false, &head_frame_id);
  next_page_id = PLeafPageGetNextPageId(head_page);

  /*
   * Make new head value.
   * Always increment head's timestamp when push or pop happen
   */
  new_head = PLeafStackMakeHead(head_timestamp, next_page_id);

  if (__sync_bool_compare_and_swap(&free_stack->head, head, new_head)) {
    /* If success to change head, return immediately */
    *frame_id = head_frame_id;

    PLeafPageSetNextPageId(head_page, PLEAF_INVALID_PAGE_ID);
    // PLeafPageSetNextPageId(head_page, 0x7FFFFFFF); why 07...?
    return head_page;
  } else {
    PLeafReleasePage(head_frame_id);
    return nullptr;
  }
}

/*
 * PLeafStackPop
 *
 * Try to pop the page in a stack.
 * We use a concurrent stack with an elimination array.
 */
static PLeafPage 
PLeafStackPop(PLeafFreeStack free_stack, 
        int *frame_id,
        PLeafGenNumber gen_no,
        int cap_index, 
        int inst_no) 
{
  PLeafPage page;
  PLeafPageId page_id;
  int pool_index;
  
  for (int i = 0; i < STACK_POP_RETRY; ++i) {
    if ((page = PLeafStackTryPop(free_stack, gen_no, frame_id)) != nullptr) {
      /* Success to get a page */
      return page;
    }
  }

  /* Fail to get a free page from stack, make new one */
  pool_index = PLeafGetPoolIndex(gen_no);
  
  ut_a(pool_index != -1);

  page_id = __sync_fetch_and_add(
                  &PLeafMetadata->pleafmeta.max_page_ids[pool_index], 1);
  page = PLeafGetPage(page_id, gen_no, true, frame_id);

  PLeafPageSetCapAndInstNo(page, cap_index, inst_no);
  PLeafPageInitBitmap(page, cap_index);
  PLeafMarkDirtyPage(*frame_id);

  return page;
}

PLeafFreePool
PLeafGetFreePool(PLeafGenNumber gen_no)
{
  PLeafFreePool ret_pool = nullptr;

  for (int i = 0; i < NPLeafPools; ++i)
  {
    if (gen_no == PLeafManager->pools[i].gen_no)
    {
      ret_pool = &PLeafManager->pools[i];
      break;
    }
  }
  ut_a(ret_pool != nullptr);
  return ret_pool;
}

PLeafGenNumber
PLeafGetLatestGenerationNumber(void)
{
  int recent_index;
  PLeafGenNumber ret_gen_no;

  recent_index = PLeafMetadata->pleafmeta.recent_index;
  ret_gen_no = PLeafMetadata->pleafmeta.generation_numbers[recent_index];
  ut_a(ret_gen_no != 0);
  return ret_gen_no;
}


static int 
PLeafGetPoolIndex(PLeafGenNumber gen_no)
{
  int ret, i;

  ret = -1;
  for (i = 0; i < NPLeafPools; ++i)
  {
    if (PLeafMetadata->pleafmeta.generation_numbers[i] == gen_no)
    {
      ret = i;
      break;
    }
  }

  return ret;
}


void
PLeafMakeNewGeneration()
{
  PLeafMeta* meta;
  PLeafGenNumber new_gen_no;
  int new_recent_index;

  meta = &PLeafMetadata->pleafmeta;
  /* Get new recent generation index */
  new_recent_index = (meta->recent_index == LEFT_POOL) ? RIGHT_POOL : LEFT_POOL;

  if (meta->generation_numbers[new_recent_index] != 0)
    return;

  new_gen_no = PLeafUpdateVersionInfo();
  ut_a(new_gen_no == meta->generation_numbers[meta->recent_index] + 1);

  meta->max_page_ids[new_recent_index] = 0;
  meta->generation_numbers[new_recent_index] = new_gen_no;
  PLeafManager->pools[new_recent_index].gen_no = new_gen_no;  

  meta->recent_index = new_recent_index;

  trx_sys->mvcc->get_latest_view_info(meta->generation_seq_no,
                                        meta->generation_max_xid);
}

/*
 * PLeafCleanOldGeneration
 *
 * Clean old generation if possible
 */
void
PLeafCleanOldGeneration(void)
{
  PLeafMeta* meta;
  ib_uint64_t oldest_seq_no;
  trx_id_t oldest_active_xid;
  int old_index;
  
  meta = &PLeafMetadata->pleafmeta;
  old_index = (meta->recent_index == LEFT_POOL) ? RIGHT_POOL : LEFT_POOL;

  /* Empty */
  if (meta->generation_numbers[old_index] == 0)
    return;

  trx_sys->mvcc->get_oldest_view_info(oldest_seq_no, oldest_active_xid);

  if (!((meta->generation_seq_no < oldest_seq_no) &&
        (meta->generation_max_xid <= oldest_active_xid)))
    return;

  /*
   * XXX:
   * It is important to acquire exclusive latch to solve race condition
   * between PLeafWritePage() and PLeafCleanOldGeneration().
   */
  rw_lock_x_lock(&pleaf_rwlocks[old_index]);
  meta->generation_numbers[old_index] = 0;
  rw_lock_x_unlock(&pleaf_rwlocks[old_index]);

  /* Initialize stacks in an old pool */
  PLeafCleanOldStacks(old_index);

  /* Initialize old file */
  PLeafCleanOldFile(old_index);
}

static void
PLeafCleanOldStacks(int old_index)
{
  PLeafFreePool free_pool;

  free_pool = &PLeafManager->pools[old_index];

  free_pool->gen_no = 0;
  PLeafInitPool(free_pool);
}

static void
PLeafCleanOldFile(int old_index)
{
  PLeafInitDataFile(old_index);
}

/*
 * PLeafNeedsNewGeneration
 * 
 * Checks if a new generation is needed
 * by comparing the current pleaf generation's maximum version number
 * and the currently existing version count collected by the EBI-tree.
 */

bool 
PLeafNeedsNewGeneration(void)
{
  bool ret;
  uint64_t num_versions;
  PLeafPageId max_page_id;
  double fraction;

  num_versions = EbiTreePtr->num_versions;

  max_page_id = PLeafMetadata->pleafmeta.max_page_ids[LEFT_POOL] +
    PLeafMetadata->pleafmeta.max_page_ids[RIGHT_POOL];

  if (max_page_id == 0)
    return false;

  fraction = ((double) num_versions) / (max_page_id * PLEAF_MAX_CAPACITY);
  ret = (fraction > PLEAF_GENERATION_THRESHOLD);

  if (fraction == 0)
    ret = true;

  return ret;
}

void
MonitorOurs(ulint tup_len)
{
  if (tup_len == 0)
    return;
  PLeafPageId max_page_id;
  max_page_id = PLeafMetadata->pleafmeta.max_page_ids[LEFT_POOL] +
    PLeafMetadata->pleafmeta.max_page_ids[RIGHT_POOL];
  ib::warn() << "[OURS] " << max_page_id  * PLEAF_PAGE_SIZE << " " <<  
    EbiTreePtr->num_versions * tup_len;
}

#endif
