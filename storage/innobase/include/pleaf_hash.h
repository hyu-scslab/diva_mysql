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

/** @file include/pleaf_hash.h
 P-Leaf Hash Interfaces

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifndef PLEAF_HASH_H
#define PLEAF_HASH_H

#include "univ.i"

extern void PLeafHashInit();
extern void PLeafHashFree();

extern ulint PLeafHashCode(const PLeafTag* tagPtr);
extern int PLeafHashLookup(const PLeafTag* tagPtr, ulint hashcode);

extern void PLeafHashInsert(const PLeafTag* tagPtr, 
        ulint hashcode, 
        int buffer_id);
 
extern void PLeafHashDelete(const PLeafTag* tagPtr,
        ulint hashcode,
        int buffer_id);
 
extern rw_lock_t* PLeafGetHashLock(ulint hashcode);

#endif /* PLEAF_HASH_H */
