/*-------------------------------------------------------------------------
 *
 * ebi_tree.c
 *
 * EBI Tree Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ebi_tree/ebi_tree.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM

#include "read0types.h"
#include "trx0sys.h"

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree_process.h"
#include "include/ebi_tree.h"
#include "include/ebi_tree_buf.h"

#include <stdlib.h>
#include <math.h>
#include <atomic>

#define NUM_VERSIONS_PER_CHUNK 1000

constexpr double W_AVG {0.999};
constexpr int W_CONST {3};

static double avg_version_lifetime {0.0};
static std::atomic_flag flag_version_lifetime {ATOMIC_FLAG_INIT};

static GarbageQueue* gc_queue {nullptr};

/* Prototypes for private functions */

/* Allocation */
static EbiNode CreateNodeWithHeight(uint32_t height);
static EbiNode CreateNode();

/* Insertion */
static EbiNode FindInsertionTargetNode(EbiTree ebitree);

/* Deletion */
static void
UnlinkNode(EbiTree ebitree, EbiNode node);
static void UnlinkFromParent(EbiNode node);
static void CompactNode(EbiTree ebitree, EbiNode node);

static void LinkProxy(EbiNode proxy, EbiNode new_proxy_target);

/* Reference Counting */
static uint32_t IncreaseRefCount(EbiNode node);
static uint32_t DecreaseRefCount(EbiNode node);

static void SetLeftBoundary(EbiNode node, ReadView* snapshot);
static void SetRightBoundary(EbiNode node, ReadView* snapshot);
static EbiNode SetRightBoundaryRecursive(EbiNode node, ReadView* snapshot);

/* Utility */
static bool HasParent(EbiNode node);
static bool HasLeftChild(EbiNode node);
static bool IsLeftChild(EbiNode node);
static bool IsLeaf(EbiNode node);

static EbiNode Sibling(EbiNode node);

static bool Overlaps(EbiNode node, trx_id_t vmin, trx_id_t vmax);

EbiTree InitEbiTree() {
  EbiTree ebitree;
  EbiNode sentinel;

  ebitree = (EbiTree) ut_zalloc_nokey(sizeof(EbiTreeData));

  sentinel = CreateNode();

  ebitree->root = ebitree->recent_node = sentinel;

  return ebitree;
}

EbiNode
CreateNode() {
  return CreateNodeWithHeight(0);
}

EbiNode
CreateNodeWithHeight(uint32_t height) {
  EbiNode node;

  /* Allocate memory */
  node = (EbiNode) ut_zalloc_nokey(sizeof(EbiNodeData));

  ut_a(node != nullptr);

  /* Initial values */
  node->parent = nullptr;
  node->left = nullptr;
  node->right = nullptr;
  node->proxy_target = nullptr;
  node->height = height;
  node->refcnt = 0;
  node->left_boundary = nullptr;
  node->right_boundary = nullptr;
  node->current_max_xid = 0;

  /* Initialize file segment */
  node->seg_id =
      EbiTreePtr->seg_id++;  // the dedicated thread, alone, creates nodes

  EbiTreeCreateSegmentFile(node->seg_id);
  node->seg_offset = 0;

  /* Version counter */
  node->num_versions = 0;

  return node;
}

void
DeleteEbiTree(EbiTree ebitree) {
  if (!ebitree)
    return;

  // Wait until all transactions leave

  // Delete each node
}

bool
NeedsNewNode(EbiTree ebitree) {
  bool state;
  ut_a((int64_t)(trx_sys->max_trx_id - 
        ebitree->recent_node->current_max_xid) >= 0);

  state = (trx_sys->max_trx_id >= ebitree->recent_node->current_max_xid + 
      avg_version_lifetime * W_CONST) ? true : false;

  if (avg_version_lifetime == 0)
    return false;

  return (ebitree->recent_node->left_boundary != nullptr && state);
}

void
InsertNode(EbiTree ebitree) {

  EbiNode target;
  EbiNode new_parent;
  EbiNode new_leaf;

  /*
   * Find the target ebi_node to perform insertion, make a new parent for the
   * target ebi_node and set its right to the newly inserted node
   *
   *   new_parent
   *    /      \
   * target  new_leaf
   *
   */
  target = FindInsertionTargetNode(ebitree);

  new_parent = CreateNodeWithHeight(target->height + 1);

  new_leaf = CreateNode();

  // Set the left bound of the new parent to the left child's left bound.
  ut_a(target->left_boundary != nullptr);
  SetLeftBoundary(new_parent, target->left_boundary);

  /*
   * Connect the original parent as the new parent's parent.
   * In the figure below, connecting nodes 'a' and 'f'.
   * (e = target, f = new_parent, g = new_leaf)
   *
   *     a                   a
   *    / \                 / \
   *   b   \       ->      b   f
   *  / \   \             / \ / \
   * c   d   e           c  d e  g
   *
   */
  if (HasParent(target)) {
    new_parent->parent = target->parent;
  }
  // f->e, f->g
  new_parent->left = target;
  new_parent->right = new_leaf;

  /*
   * At this point, the new nodes('f' and 'g') are not visible
   * since they are not connected to the original tree.
   */
  os_rmb;

  // a->f
  if (HasParent(target)) {
    target->parent->right = new_parent;
  }
  // e->f, g->f
  target->parent = new_parent;
  new_leaf->parent = new_parent;

  // If the target node is root, the root is changed to the new parent.
  if (target == ebitree->root) {
    ebitree->root = new_parent;
  }

  os_rmb;

  // Change the last leaf node to the new right node.
  ebitree->recent_node = new_leaf;
}

static EbiNode 
FindInsertionTargetNode(EbiTree ebitree) {

  EbiNode tmp;
  EbiNode parent;
  EbiNode left;
  EbiNode right;

  tmp = ebitree->recent_node;
  parent = tmp->parent;

  while (parent != nullptr) {
    left = parent->left;
    right = parent->right;

    if (left->height > right->height) {
      // Unbalanced, found target node.
      break;
    } else {
      tmp = parent;
      parent = tmp->parent;
    }
  }

  return tmp;
}

/**
 * Reference Counting
 */
EbiNode
EbiIncreaseRefCount(ReadView* snapshot) {
  EbiTree tree;
  EbiNode recent_node;
  EbiNode sibling, prev_node;
  uint32_t refcnt;

  tree = EbiTreePtr->ebitree;
  recent_node = tree->recent_node;

  refcnt = IncreaseRefCount(recent_node);

  // The first one to enter the current node should set the boundary.
  if (refcnt == 1) {
    // The next epoch's opening transaction will decrease ref count twice.
    refcnt = IncreaseRefCount(recent_node);

    sibling = Sibling(recent_node);

    SetLeftBoundary(recent_node, snapshot);

    // When the initial root node stands alone, sibling could be NULL.
    if (sibling != nullptr) {
      SetRightBoundary(sibling, snapshot);
      os_rmb;
      prev_node = SetRightBoundaryRecursive(sibling, snapshot);

      // May delete the last recent node if there's no presence of any xacts.
      EbiDecreaseRefCount(prev_node);
    }

    recent_node->current_max_xid = trx_sys->max_trx_id;
    ut_a(recent_node->current_max_xid != 0);
  }

  return recent_node;
}

static uint32_t
IncreaseRefCount(EbiNode node) {
  uint32_t ret;
  ret = __sync_add_and_fetch(&node->refcnt, 1);
  return ret;
}

static void
SetLeftBoundary(EbiNode node, ReadView* snapshot) {
  ut_a(!node->left_boundary);
  node->left_boundary = UT_NEW_NOKEY(ReadView());
  ut_a(node->left_boundary);

  node->left_boundary->copy_prepare_simple(*snapshot);
  node->left_boundary->copy_complete_simple();
}

static void
SetRightBoundary(EbiNode node, ReadView* snapshot) {
  ut_a(!node->right_boundary);
  node->right_boundary = UT_NEW_NOKEY(ReadView());
  ut_a(node->right_boundary);

  node->right_boundary->copy_prepare_simple(*snapshot);
  node->right_boundary->copy_complete_simple();
}


static EbiNode
SetRightBoundaryRecursive(EbiNode node, ReadView* snapshot) {
  EbiNode ret_node;
  ret_node = node;
  
  while (ret_node->right != nullptr) {
    ret_node = ret_node->right;
    SetRightBoundary(ret_node, snapshot);
  }
  return ret_node;
}

void
EbiDecreaseRefCount(EbiNode node) {
  uint32_t refcnt;

  refcnt = DecreaseRefCount(node);

  if (refcnt == 0) {
    Enqueue(EbiTreePtr->unlink_queue, node);
  }
}

static uint32_t
DecreaseRefCount(EbiNode node) {
  uint32_t ret;
  ret = __sync_sub_and_fetch(&node->refcnt, 1);
  return ret;
}

void
UnlinkNodes(EbiTree ebitree, TaskQueue unlink_queue) { 
  EbiNode tmp;

  tmp = Dequeue(unlink_queue);

  while (tmp != nullptr) {
    // Logical deletion
    UnlinkNode(ebitree, tmp);
    gc_queue->push(tmp);

    tmp = Dequeue(unlink_queue);
  }
}

static void
UnlinkNode(EbiTree ebitree, EbiNode node) {

  // Logical deletion, takes it off from the EBI-tree
  UnlinkFromParent(node);

  // Compaction
  CompactNode(ebitree, node->parent);
}

static void
UnlinkFromParent(EbiNode node) {
  EbiNode parent, curr;
  uint64 num_versions;

  parent = node->parent;

  if (IsLeftChild(node)) {
    parent->left = nullptr;
  } else {
    parent->right = nullptr;
  }

  curr = node;

  while (curr != nullptr) {
    /* Version counter */
    num_versions = curr->num_versions;

    num_versions = num_versions - (num_versions % NUM_VERSIONS_PER_CHUNK);
    __sync_fetch_and_sub(&EbiTreePtr->num_versions, num_versions);
    curr = curr->proxy_target;
  }
}

static void
CompactNode(EbiTree ebitree, EbiNode node) {
  EbiNode tmp;
  EbiNode proxy_target;
  uint32_t original_height;

  proxy_target = node;

  if (HasParent(node) == false) {
    // When the root's child is being compacted
    if (HasLeftChild(node)) {
      LinkProxy(node->left, proxy_target);
      ebitree->root = node->left;
    } else {
      LinkProxy(node->right, proxy_target);
      ebitree->root = node->right;
    }
    ebitree->root->parent = nullptr;
  } else {
    EbiNode parent;
    EbiNode tmp_ptr;

    parent = node->parent;

    // Compact the one-and-only child and its parent
    if (IsLeftChild(node)) {
      if (HasLeftChild(node)) {
        LinkProxy(node->left, proxy_target);
        parent->left = node->left;
      } else {
        LinkProxy(node->right, proxy_target);
        parent->left = node->right;
      }
      tmp = parent->left;
    } else {
      if (HasLeftChild(node)) {
        LinkProxy(node->left, proxy_target);
        parent->right = node->left;
      } else {
        LinkProxy(node->right, proxy_target);
        parent->right = node->right;
      }
      tmp = parent->right;
    }
    tmp->parent = node->parent;

    // Parent height propagation
    tmp_ptr = node->parent;
    while (tmp_ptr != nullptr) {
      EbiNode curr, left, right;

      curr = tmp_ptr;
      left = curr->left;
      right = curr->right;

      original_height = curr->height;

      curr->height = ut_max(left->height, right->height) + 1;

      if (curr->height == original_height) {
        break;
      }

      tmp_ptr = curr->parent;
    }
  }
}

static void
LinkProxy(EbiNode proxy, EbiNode new_proxy_target) {

  if (proxy->proxy_target) {
    new_proxy_target->proxy_target = proxy->proxy_target;
  }

  proxy->proxy_target = new_proxy_target;
}


EbiNode
Sift(trx_id_t vmin, trx_id_t vmax) {
  EbiTree ebitree;
  EbiNode curr, left, right;
  bool left_includes, right_includes;
  bool left_exists, right_exists;
  uint64_t my_slot;

  ebitree = EbiTreePtr->ebitree;

  my_slot = gc_queue->increase_ref_count();
  curr = ebitree->root;

  ut_a(curr != nullptr);

  /* If root's left boundary doesn't set yet, return immediately, or 
   the version is already dead, may be cleaned. */
  if (!curr->left_boundary || !Overlaps(curr, vmin, vmax)) {
    curr = nullptr;
    goto func_exit;
  }

  while (curr != nullptr && !IsLeaf(curr)) {
    left = curr->left;
    right = curr->right;

    left_exists = (left && left->left_boundary);
    right_exists = (right && right->left_boundary);

    if (!left_exists && !right_exists) {
      curr = nullptr;
      goto func_exit;
    } else if (!left_exists) {
      // Only the left is null and the version does not fit into the right
      if (!Overlaps(right, vmin, vmax)) {
        curr = nullptr;
        goto func_exit;
      } else {
        curr = curr->right;
      }
    } else if (!right_exists) {
      // Only the right is null and the version does not fit into the left
      if (!Overlaps(left, vmin, vmax)) {
        curr = nullptr;
        goto func_exit;
      } else {
        curr = curr->left;
      }
    } else {
      // Both are not null
      left_includes = Overlaps(left, vmin, vmax);
      right_includes = Overlaps(right, vmin, vmax);

      if (left_includes && right_includes) {
        // Overlaps both child, current interval is where it fits
        break;
      } else if (left_includes) {
        curr = curr->left;
      } else if (right_includes) {
        curr = curr->right;
      } else {
        curr = nullptr;
        goto func_exit;
      }
    }
  }
func_exit:
  gc_queue->decrease_ref_count(my_slot);
  return curr;
}

static bool
Overlaps(EbiNode node, trx_id_t vmin, trx_id_t vmax) {
  ReadView *left_snap, *right_snap;

  left_snap = node->left_boundary;
  right_snap = node->right_boundary;

  ut_a(left_snap != nullptr);
    
  if (right_snap != nullptr) {
    return (!left_snap->changes_visible_simple(vmax) &&
          right_snap->changes_visible_simple(vmin));
  } else {
    return !left_snap->changes_visible_simple(vmax);
  }
}

EbiTreeVersionOffset
EbiTreeSiftAndBind(
    trx_id_t vmin,
    trx_id_t vmax,
    ulint tuple_size,
    const void* tuple) {

  EbiNode node;
  EbiTreeSegmentId seg_id;
  EbiTreeSegmentOffset seg_offset;
  bool found;
  EbiTreeVersionOffset ret;
  uint64_t num_versions;

  node = Sift(vmin, vmax);

  if (!node) {
    /* Reclaimable */
    return EBI_TREE_INVALID_VERSION_OFFSET;
  }

  /* We currently forbid tuples with sizes that are larger than the page size */
  ut_a(tuple_size <= EBI_TREE_SEG_PAGESZ);

  seg_id = node->seg_id;
  do {
    seg_offset = __sync_fetch_and_add(&node->seg_offset, tuple_size);

    /* Checking if the tuple could be written within a single page */
    found = seg_offset / EBI_TREE_SEG_PAGESZ ==
            (seg_offset + tuple_size - 1) / EBI_TREE_SEG_PAGESZ;
  } while (!found);

  // Write version to segment
  EbiTreeAppendVersion(seg_id, seg_offset, tuple_size, tuple);

  if (flag_version_lifetime.test_and_set(std::memory_order_acquire)) {

    avg_version_lifetime = avg_version_lifetime * W_AVG +
          abs((int64_t)(vmax - vmin)) * ((double)1 - W_AVG);

    flag_version_lifetime.clear(std::memory_order_release);
  }
  num_versions = __sync_add_and_fetch(&node->num_versions, 1);

  // Update global counter if necessary
  if (num_versions % NUM_VERSIONS_PER_CHUNK == 0) {
    __sync_fetch_and_add(&EbiTreePtr->num_versions, NUM_VERSIONS_PER_CHUNK);
  }

  ret = EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset);

  return ret;
}

int
EbiTreeLookupVersion(
    EbiTreeVersionOffset version_offset,
    ulint tuple_size,
    rec_t** ret_value) {
  EbiTreeSegmentId seg_id;
  EbiTreeSegmentOffset seg_offset;
  int buf_id;

  seg_id = EBI_TREE_VERSION_OFFSET_TO_SEG_ID(version_offset);
  seg_offset = EBI_TREE_VERSION_OFFSET_TO_SEG_OFFSET(version_offset);

  ut_a(seg_id >= 1);

  // Read version to ret_value
  buf_id = EbiTreeReadVersionRef(seg_id, seg_offset, tuple_size, ret_value);

  return buf_id;
}

bool
EbiTreeSegIsAlive(EbiTree ebitree, EbiTreeSegmentId seg_id) {
  EbiNode curr;

  curr = ebitree->root;

  while (!IsLeaf(curr)) {
    if (curr->seg_id == seg_id) {
      return true;
    }

    if (seg_id < curr->seg_id) {
      curr = curr->left;
    } else {
      curr = curr->right;
    }

    /* It has been concurrently removed by the EBI-tree process */
    if (!curr) {
      return false;
    }
  }

  ut_a(IsLeaf(curr));

  while (curr != nullptr) {

    if (curr->seg_id == seg_id) {
      return true;
    }
    curr = curr->proxy_target;
  }
  return false;
}

/**
 * Utility Functions
 */

static bool
HasParent(EbiNode node) {
  return (node->parent != nullptr);
}

static bool
IsLeftChild(EbiNode node) {
  return (node->parent->left == node);
}

static bool
HasLeftChild(EbiNode node) {
  return (node->left != nullptr);
}

static bool
IsLeaf(EbiNode node) {
  return (node->height == 0);
}

static EbiNode
Sibling(EbiNode node) {
  EbiNode parent;

  if (HasParent(node)) {
    parent = node->parent;
    if (IsLeftChild(node)) {
      return parent->right;
    } else {
      return parent->left;
    }
  } else {
    return nullptr;
  }
}

void EbiTreeGCQueueInit() {
  gc_queue = UT_NEW_NOKEY(GarbageQueue());
}

void EbiTreeGCQueueFree() {
  ut_a(gc_queue);
  UT_DELETE(gc_queue);
}

void GCQueueWork() {
  ut_a(gc_queue);
  gc_queue->changes_slot_if_possible();
}


/** Garbage Queue */
GarbageQueue::GarbageQueue() {
  this->current_slot = 0;
  memset(ref_count, 0x00, sizeof(uint64_t) * 2);
}

void GarbageQueue::push(EbiNode node) {
  this->garbage_queue[current_slot].push(node);
}

void GarbageQueue::changes_slot_if_possible() {
  uint64_t prev_slot = (this->current_slot + 1) % 2;

  if (this->ref_count[prev_slot] == 0) {
    /** Physical delete all nodes in queue[cur_slot]. */
    pop_all(prev_slot);

    this->current_slot = prev_slot;
    os_wmb;
  }
}

void GarbageQueue::delete_node(EbiNode node) {
  EbiNode del_node = node;
  EbiNode tmp_node;
  
  while (del_node) {
    tmp_node = del_node;
    del_node = del_node->proxy_target;
    EbiTreeRemoveSegmentFile(tmp_node->seg_id);

    ut_a(tmp_node->left_boundary);
    UT_DELETE(tmp_node->left_boundary);
    if (tmp_node->right_boundary)
      UT_DELETE(tmp_node->right_boundary);
    ut_free(tmp_node);
  }
}

void GarbageQueue::pop_all(uint64_t cur_slot) {
  EbiNode del_node;
  while (!garbage_queue[cur_slot].empty()) {
    del_node = garbage_queue[cur_slot].front();
    delete_node(del_node);
    garbage_queue[cur_slot].pop();
  }
}

uint64_t GarbageQueue::increase_ref_count() {
  os_rmb;
  uint64_t ret_slot = this->current_slot;
  __sync_fetch_and_add(&ref_count[ret_slot], 1);
  return ret_slot;
}

void GarbageQueue::decrease_ref_count(uint64_t my_slot) {
  __sync_fetch_and_sub(&ref_count[my_slot], 1);
}
#endif
