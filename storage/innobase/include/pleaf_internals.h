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

/** @file include/pleaf_internals.h
 P-Leaf Internal Interfaces

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifndef PLEAF_INTERNALS_H
#define PLEAF_INTERNALS_H

#include "univ.i"

/* pleaf_reader.c */
#define PLEAF_LOOKUP_FORWARD  (0)
#define PLEAF_LOOKUP_BACKWARD (1)
#define PLEAF_LOOKUP_FOUND    (2)

/* pleaf_writer.c */
#define PLEAF_APPEND_NOTHING  (0)
#define PLEAF_APPEND_ERROR    (1)
#define PLEAF_APPEND_SUCCESS  (2)

#define MAX_ARRAY_ACCESSED  (5)
#define LAST_ARRAY_ACCESSED (1)

#define PLEAF_APPEND_DONE     (0)
#define PLEAF_APPEND_CONTRACT (1)
#define PLEAF_APPEND_COMPACT  (2)
#define PLEAF_APPEND_EXPAND   (3)

#define PLeafMakeOffset(gen_no, offset) \
  ((uint64_t)(((uint64_t)gen_no << 48) | offset))

#define GetVersionCount(head, tail, cap) \
  ((head <= tail) ? tail - head : tail - head + (2 * cap))

typedef struct 
{
  PLeafPage page;
  int array_index;
  int frame_id;
} PLeafTempInfoData;

typedef PLeafTempInfoData* PLeafTempInfo;

extern int PLeafIsVisible(ReadView* snapshot, 
                trx_id_t xmin, 
                trx_id_t xmax);

extern bool PLeafCheckVisibility(trx_id_t xmin, 
                trx_id_t xmax);

//extern void PLeafGetVersionInfo(PLeafVersionId version_id,
//                trx_id_t* xmin, 
//                trx_id_t* xmax);

extern bool PLeafCheckAppendness(int cap, uint16_t head, uint16_t tail);
extern bool PLeafCheckEmptiness(uint16_t head, uint16_t tail);


extern bool PLeafLookupVersion(PLeafPage page, 
                  PLeafOffset* offset, 
                  ReadView* snapshot);

extern int PLeafAppendVersion(PLeafOffset offset, 
                  PLeafOffset* ret_offset,
                  trx_id_t xmin, trx_id_t xmax, 
                  PLeafVersionOffset version_offset);

#endif /* PLEAF_INTERNALS_H */
