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

/** @file pleaf/pleaf_stack_helper.cc 
 P-Leaf Concurrent Stack Helper

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifdef J3VM

#include <time.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <assert.h>

#include "include/pleaf_stack_helper.h"

/*
 * ElimArrayExchange
 *
 * Conventional exchange function in elimination array
 */
static uint64_t ElimArrayExchange(uint64_t* exchanger, uint64_t value) 
{
  uint64_t exchanger_value;
  uint64_t new_value;
  int retry;

  retry = 0;

  do {
    /* Read exchanger's value */
    exchanger_value = *exchanger;

    switch (GetExchangerStatus(exchanger_value)) {
      case EL_EMPTY:
        new_value = SetExchangerNewValue(value, EL_WAITING);
        /* Try to set my value */
        if (__sync_bool_compare_and_swap(
              exchanger, exchanger_value, new_value)) {
          
          /* If success, wait for other exchangers */
          do {
            retry++;
            sleep(ELIM_ARRAY_DUR);
            exchanger_value = *exchanger;

            /* If someone exchanges value, set it to null and return */
            if (GetExchangerStatus(exchanger_value) == EL_BUSY) {
              *exchanger = EXCHANGER_INIT;
              return GetExchangerValue(exchanger_value);
            }
          } while (retry <= 2);

          /* After timeout, one more chance to exchange values */
          if (__sync_bool_compare_and_swap(
                exchanger, new_value, EXCHANGER_INIT)) {
            return EXCHANGER_FAIL;
          }

          exchanger_value = *exchanger;
          *exchanger = EXCHANGER_INIT;

          return GetExchangerValue(exchanger_value);
        }
        break;

      case EL_WAITING:
        new_value = SetExchangerNewValue(value, EL_BUSY);
        /* Try to set my value */
        if (__sync_bool_compare_and_swap(
              exchanger, exchanger_value, new_value)) {

          /* If success, return its old value */
          return GetExchangerValue(exchanger_value); 
        }

        break;
      case EL_BUSY:
        break;
      default:
        ut_a(false);
    }
  } while(0);

  return EXCHANGER_FAIL;
  ut_a(false);
}

/*
 * ElimArrayVisit
 *
 * Conventional visit function in elimination array
 *
 * Called only in stack push and pop
 */
uint64_t ElimArrayVisit(EliminationArray elimination_array, uint64_t value) 
{
  int pos;
  /* static duration */
  srand((unsigned int)time(NULL));
  pos = rand() % N_ELIMINATION_ARRAY;
  return ElimArrayExchange(&elimination_array->arr[pos], value);
}

#endif
