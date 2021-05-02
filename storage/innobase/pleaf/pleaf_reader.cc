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

/** @file pleaf/pleaf_reader.cc
 P-Leaf Internal(Reader) Interfaces

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifdef J3VM

#include "read0types.h"

#include "include/pleaf_bufpage.h"
#include "include/pleaf_internals.h"

#include <stdbool.h>
#include <assert.h>

/*
 * PLeafLookupVersion
 *
 * Lookup the visible version locator(or offset in indirect array).
 */
bool
PLeafLookupVersion(PLeafPage page, 
              PLeafOffset* offset, 
              ReadView* snapshot)
{

  PLeafVersion first_version, version;
  //PLeafVersionId version_id;
  PLeafVersionIndex version_index;
  PLeafVersionIndexData version_index_data;
  trx_id_t xmin;
  trx_id_t xmax;
  int status;
  int start, mid, end;
  uint16_t version_head, version_tail;
  bool version_found;
  int capacity, array_index;

  /* Initialize return value */
  version_found = false;

  /* Get capacity */
  capacity = PLeafPageGetCapacity(page);
  /* Get array index */
  array_index = PLeafPageGetArrayIndex(capacity, *offset);

  /* Initialize offset value */
  *offset = PLEAF_INVALID_VERSION_OFFSET;

  /* Get version index and its data */
  version_index = PLeafPageGetVersionIndex(page, array_index);
  version_index_data = *version_index;

  if (PLeafGetVersionType(version_index_data) == PLEAF_DIRECT_ARRAY) 
    version_found = true;

  /* Get version head and tail */
  version_head = PLeafGetVersionIndexHead(version_index_data);
  version_tail = PLeafGetVersionIndexTail(version_index_data);

  /* If empty, return immediately */
  if (PLeafCheckEmptiness(version_head, version_tail)) 
    return true;

  /* Get the very first version in the version array */
  first_version = PLeafPageGetFirstVersion(page, array_index, capacity); 

  /* Get the head version */
  version = PLeafPageGetVersion(first_version, version_head % capacity);

  /* Get xmin and xmax value */
  //PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);
  xmin = PLeafGetXmin(version);
  xmax = PLeafGetXmax(version);

  /*
   * !!! 
   * Check visibility with transaction's snapshot.
   * If return status is PLEAF_LOOKUP_BACKWARD, the contention between readers 
   * and writer occurs. It can be solved by reading and checking head value 
   * one more time.
   */
  if((status = PLeafIsVisible(snapshot, xmin, xmax)) == 
                                            PLEAF_LOOKUP_BACKWARD) 
  {
    return true;
    /* Version head should be changed, and it must guarantee */
    version_index_data = *version_index;
    ut_a(version_head != PLeafGetVersionIndexHead(version_index_data));

    /* Read a version head one more. No need to read a version tail */
    version_head = PLeafGetVersionIndexHead(version_index_data);

    /* If empty, return immediately */
    if (PLeafCheckEmptiness(version_head, version_tail)) 
      return true;

    /* Get the head version */
    version = PLeafPageGetVersion(first_version, version_head % capacity);
    //ut_a(version_id != PLeafGetVersionId(version));
    ut_a(xmin != PLeafGetXmin(version) || xmax != PLeafGetXmax(version));
    
    /* Get xmin and xmax value */
    //PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);
    xmin = PLeafGetXmin(version);
    xmax = PLeafGetXmax(version);
  
    /* Check visibility */
    status = PLeafIsVisible(snapshot, xmin, xmax);
  }

  /* If found, return immediately */
  if (status == PLEAF_LOOKUP_FOUND) 
  {
    *offset = PLeafGetVersionOffset(version);
    return version_found;
  }
  
  /* From here, we must guarantee to search forward */
  ut_a(status != PLEAF_LOOKUP_BACKWARD);
  
  /* We already check visibility of version_head's version, so skip it */
  version_head = (version_head + 1) % (2 * capacity);

  /* Circular array to linear array */
  if (version_tail < version_head) 
  {
    version_tail += (2 * capacity);
  } 
  else if (version_tail == version_head) 
  {
    return true;
  }

  /*
   * Binary search in version array
   */
  start = version_head;
  end = version_tail - 1;

  ut_a(end >= 0);

  while (end >= start) 
  {
    mid = (start + end) / 2;

    version = PLeafPageGetVersion(first_version, mid % capacity);
    //PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);
    xmin = PLeafGetXmin(version);
    xmax = PLeafGetXmax(version);

    switch (PLeafIsVisible(snapshot, xmin, xmax)) 
    {
      case PLEAF_LOOKUP_FOUND:
        *offset = PLeafGetVersionOffset(version);
        return version_found;

      case PLEAF_LOOKUP_FORWARD:
        start = mid + 1;
        break;

      case PLEAF_LOOKUP_BACKWARD:
        end = mid - 1;
        break;

      default:
        ut_a(false);
    }
  }
  return true;
}

#endif
