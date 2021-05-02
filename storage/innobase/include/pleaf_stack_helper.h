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

/** @file include/pleaf_stack_helper.h 
 P-Leaf Concurrent Stack Helper

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifndef PLEAF_STACK_HELPER_H
#define PLEAF_STACK_HELPER_H

#include "univ.i"

#define N_ELIMINATION_ARRAY (8)
#define ELIM_ARRAY_DUR (double)(0.000001) // micro-sec

#define EXCHANGER_INIT (0)
#define EXCHANGER_FAIL ((uint64_t)(-1))

#define GetExchangerStatus(v) \
  ((uint32_t)((v & ~HELPER_VALUE_MASK) >> 32))

#define HELPER_VALUE_MASK (0x00000000FFFFFFFFULL)

#define GetExchangerValue(v) \
  (v & HELPER_VALUE_MASK)

#define SetExchangerNewValue(v, s) \
  ((((uint64_t)s) << 32) | v)

#define EL_EMPTY 0
#define EL_WAITING 1
#define EL_BUSY 2

typedef struct 
{
  uint64_t arr[N_ELIMINATION_ARRAY];
} EliminationArrayData;

typedef EliminationArrayData* EliminationArray;

extern uint64_t ElimArrayVisit(EliminationArray elim_array, 
                        uint64_t value);

#endif /* PLEAF_STACK_HELPER_H */
