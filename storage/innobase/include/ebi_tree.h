/*-------------------------------------------------------------------------
 *
 * ebi_tree.h
 *    EBI Tree
 *
 *
 *
 * src/include/storage/ebi_tree.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_H
#define EBI_TREE_H

#include "univ.i"
#include <queue>

/*
 * Actual EBI tree structure.
 * */
typedef uint32_t EbiTreeSegmentId;
typedef uint64_t EbiTreeSegmentOffset;
typedef uint32_t EbiTreeSegmentPageId;
typedef uint64_t EbiTreeVersionOffset;

/* Maximum number of segments used as EBI tree segment */
#define EBI_TREE_INVALID_SEG_ID ((EbiTreeSegmentId)(0))
#define EBI_TREE_INVALID_VERSION_OFFSET ((uint64)(-1))

#define EBI_TREE_SEG_ID_MASK (0xFFFFFF0000000000ULL)
#define EBI_TREE_SEG_OFFSET_MASK (0x000000FFFFFFFFFFULL)
#define EBI_TREE_VERSION_OFFSET_TO_SEG_ID(version_offset) \
  ((version_offset & EBI_TREE_SEG_ID_MASK) >> 40)

#define EBI_TREE_VERSION_OFFSET_TO_SEG_OFFSET(version_offset) \
  (version_offset & EBI_TREE_SEG_OFFSET_MASK)

#define EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset) \
  ((((uint64)(seg_id)) << 40) | seg_offset)

typedef struct EbiNodeData* EbiNode;

typedef struct EbiNodeData {
  EbiNode parent;
  EbiNode left;
  EbiNode right;
  EbiNode proxy_target;

  uint32_t height;
  uint32_t refcnt;

  ReadView* left_boundary; /* copy_prepare , copy_complete */
  ReadView* right_boundary;

  EbiTreeSegmentId seg_id;       /* file segment */
  uint64_t seg_offset;   /* aligned version offset */
  uint64_t num_versions; /* number of versions */

  trx_id_t current_max_xid;
} EbiNodeData;


typedef struct EbiTreeData {
  EbiNode root;
  EbiNode recent_node;
} EbiTreeData;

typedef struct EbiTreeData* EbiTree;

/* Public functions */

extern EbiNode EbiIncreaseRefCount(ReadView* snapshot);
extern void EbiDecreaseRefCount(EbiNode node);

extern EbiTree InitEbiTree();
extern void DeleteEbiTree(EbiTree ebitree);

extern void InsertNode(EbiTree ebitree);
extern void UnlinkNodes(
    EbiTree ebitree,
    TaskQueue unlink_queue);

extern bool NeedsNewNode(EbiTree ebitree);

extern EbiNode Sift(trx_id_t vmin, trx_id_t vmax);

extern EbiTreeVersionOffset EbiTreeSiftAndBind(
    trx_id_t xmin,
    trx_id_t xmax,
    ulint tuple_size,
    const void* tuple);

extern int EbiTreeLookupVersion(
    EbiTreeVersionOffset version_offset,
    ulint tuple_size,
    rec_t** ret_value);

extern bool EbiTreeSegIsAlive(EbiTree ebitree, EbiTreeSegmentId seg_id);

extern void EbiTreeGCQueueInit();
extern void EbiTreeGCQueueFree();
extern void GCQueueWork();

class GarbageQueue {
  private:
    std::queue<EbiNode> garbage_queue[2];
    uint64_t ref_count[2];
    uint64_t current_slot;

  public:
    void push(EbiNode node);
    void changes_slot_if_possible();

    uint64_t increase_ref_count();
    void decrease_ref_count(uint64_t my_slot);

    GarbageQueue();
    ~GarbageQueue(){};

  private:
    // Prevent copying
    GarbageQueue(const GarbageQueue&);
    GarbageQueue &operator=(const GarbageQueue &);

    void pop_all(uint64_t cur_slot);
    void delete_node(EbiNode node);
};

#endif /* EBI_TREE_H */
