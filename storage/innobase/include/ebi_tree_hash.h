/*-------------------------------------------------------------------------
 *
 * ebi_tree_hash.h
 *    Hash table definitions for mapping EbiTreePage to EbiTreeBuffer indexes.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ebi_tree_hash.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_HASH_H
#define EBI_TREE_HASH_H

#include "univ.i"

extern void EbiTreeHashInit();
extern void EbiTreeHashFree();

extern ulint EbiTreeHashCode(const EbiTreeBufTag* tagPtr);

extern int EbiTreeHashLookup(const EbiTreeBufTag* tagPtr, ulint hashcode);
extern void EbiTreeHashInsert(const EbiTreeBufTag* tagPtr, 
                                ulint hashcode, int buffer_id);
extern void EbiTreeHashDelete(const EbiTreeBufTag* tagPtr, 
                                ulint hashcode, int buffer_id);

extern rw_lock_t* EbiGetHashLock(ulint hashcode);

#endif /* EBI_TREE_HASH_H */
