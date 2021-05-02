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

/** @file include/pleaf_buf.h
 P-Leaf Buffer Structure

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifndef PLEAF_BUF_H
#define PLEAF_BUF_H

#include "univ.i"

/* PLeaf Buffer Size */
#ifdef PLEAF_NUM_PAGE
constexpr int NPLeafBuffers = PLEAF_NUM_PAGE;
#else
constexpr int NPLeafBuffers = 1000;
#endif /* PLEAF_NUM_PAGE */

/* PLeaf Number of Instances (<= 15) */
#ifdef PLEAF_NUM_INSTANCE
constexpr int NPLeafInstances = PLEAF_NUM_INSTANCE;
#else
constexpr int NPLeafInstances = 4;
#endif /* PLEAF_NUM_INSTANCE */

/* PLeaf Initialize Pages per Instance */
#ifdef PLEAF_INIT_PAGES
constexpr int NPLeafInitPages = PLEAF_INIT_PAGES;
#else
constexpr int NPLeafInitPages = 0;
#endif /* PLEAF_INIT_PAGES */

/* PLeaf Hash Partitions */
#ifdef PLEAF_NUM_PARTITION
constexpr int NPLeafPartitions = PLEAF_NUM_PARTITION;
#else
constexpr int NPLeafPartitions = 16;
#endif /* PLEAF_NUM_PARTITION */

/* Base 16MB */
#define PLEAF_INIT_FILE_SIZE (PLEAF_PAGE_SIZE * 1024)

#define NPLeafPools (2)

#define LEFT_POOL   (0)
#define RIGHT_POOL  (1)

#define InvalidGid ((PLeafGenNumber)(-1))

#define MY_MAX_TRX_ID (1ULL << (DATA_TRX_ID_LEN * CHAR_BIT))
#define PLEAF_GENERATION_THRESHOLD (0.1)

#define SIZEOF_VOID_P ((sizeof(void*)))
/*
 * PLeaf buffer tag identifies which disk block the buffer contains 
 */
typedef struct pleaftag
{
  PLeafPageId page_id;
  PLeafGenNumber gen_no;
  int16_t pad;
} PLeafTag;

#define CLEAR_PLEAFTAG(a) \
  (\
   (a).page_id = 0, \
   (a).gen_no = 0, \
   (a).pad = 0 \
  )

#define INIT_PLEAF_TAG(a, xx_page_id, xx_gen_no) \
  (\
   (a).page_id = xx_page_id, \
   (a).gen_no = xx_gen_no \
  )

#define PLEAFTAGS_EQUAL(a, b) \
  (\
   (a).page_id == (b).page_id && \
   (a).gen_no == (b).gen_no \
  )

typedef struct PLeafDesc
{
  /* Id of the cached page. gen_no 0  means this entry is unused */
  PLeafTag tag;

  /* whether the page is not yet synced */
  bool is_dirty;

  /* Cache entry with refcount > 0 cannot be evicted */
  uint32_t refcount;
  //pg_atomic_uint32 refcount;
} PLeafDesc;

#define PLEAFDESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union PLeafDescPadded
{
  PLeafDesc pleafdesc;
  char    pad[PLEAFDESC_PAD_TO_SIZE];
} PLeafDescPadded;  

typedef struct PLeafMeta
{
  /*
   * Indicate the cache entry which might be a victim for allocating
   * a new page. Need to use fetch-and-add on this so that multiple
   * transactions can allocate/evict cache entries concurrently
   */
  uint64_t eviction_rr_idx;
  //pg_atomic_uint64  eviction_rr_idx;

  /* Page id not allocated to transactions yet */
  PLeafPageId    max_page_ids[NPLeafPools];

  /* Recent array index: 0 or 1 */
  int recent_index;

  ib_uint64_t generation_seq_no;
  trx_id_t    generation_max_xid;

  /* PLeaf File Version Number */
  PLeafGenNumber     generation_numbers[NPLeafPools];

} PLeafMeta;

#define PLEAFMETA_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union PLeafMetaPadded
{
  PLeafMeta pleafmeta;
  char    pad[PLEAFMETA_PAD_TO_SIZE];
} PLeafMetaPadded;

#define GetPLeafDescriptor(id) (&PLeafDescriptors[(id)].pleafdesc)

#define NUM_OF_STACKS (6)

#define STACK_POP_RETRY (2)

#define PLeafStackGetTimestamp(x) \
  ((uint32_t)((x & ~PLEAF_PAGE_ID_MASK) >> 32))

#define PLeafStackGetPageId(x) \
  ((uint32_t)(x & PLEAF_PAGE_ID_MASK))

#define PLeafStackMakeHead(timestamp, page_id) \
  ((((uint64_t)(timestamp + 1) << 32)) | page_id)


typedef uint32_t PLeafStackTimestamp;

typedef struct {
  /*
   * Bits in head (msb to lsb)
   * 4byte : timestamp
   * 4byte : page id
   */
  PLeafPageMetadata head;
  char pad[56];

  EliminationArrayData elim_array;
} PLeafFreeStackData;

typedef PLeafFreeStackData* PLeafFreeStack;

typedef struct {
  PLeafFreeStackData free_stacks[NUM_OF_STACKS];
} PLeafFreeInstanceData;

typedef PLeafFreeInstanceData* PLeafFreeInstance;

typedef struct {
  /*
   * When requesting page in the first place,
   * use round-robin manner
   */
  uint64_t rr_counter;
  //pg_atomic_uint64 rr_counter;
  PLeafGenNumber gen_no;

  char pad[48];

  PLeafFreeInstance free_instances;
} PLeafFreePoolData;

typedef PLeafFreePoolData* PLeafFreePool;


typedef struct {
  PLeafFreePoolData pools[NPLeafPools];
} PLeafFreeManagerData;

typedef PLeafFreeManagerData* PLeafFreeManager;

extern PLeafFreePool PLeafGetFreePool(PLeafGenNumber gen_no);
extern PLeafGenNumber PLeafGetLatestGenerationNumber(void);

extern bool PLeafIsOffsetValid(PLeafOffset offset);

extern bool PLeafIsGenerationNumberValidInUpdate(PLeafGenNumber left, 
                      PLeafGenNumber right);

extern void PLeafIsGenerationNumberValidInLookup(PLeafGenNumber gen_no); 

extern PLeafGenNumber PLeafGetOldGenerationNumber(void);

extern void PLeafMarkDirtyPage(int frame_id);

extern PLeafPage PLeafGetPage(PLeafPageId page_id,
                      PLeafGenNumber gen_no,
                      bool is_new, 
                      int *frame_id);

extern void PLeafReleasePage(int frame_id);

extern PLeafPage PLeafGetFreePage(int *frame_id,
                      PLeafFreePool free_pool);

extern PLeafPage PLeafGetFreePageWithCapacity(int* frame_id, 
                      PLeafFreePool free_pool,
                      int cap_index, 
                      int inst_no);

extern PLeafOffset PLeafFindFreeSlot(PLeafPage page, 
                      int frame_id, 
                      PLeafFreePool free_pool,
                      uint32_t type);

extern void PLeafReleaseFreeSlot(PLeafPage page, 
                      int frame_id, 
                      PLeafFreePool free_pool,
                      int capacity, 
                      int array_index);

extern PLeafPageId PLeafFrameToPageId(int frame_id);

extern void PLeafMakeNewGeneration(void);
extern void PLeafCleanOldGeneration(void);

extern bool PLeafNeedsNewGeneration(void);

#endif /* PLEAF_BUF_H */
