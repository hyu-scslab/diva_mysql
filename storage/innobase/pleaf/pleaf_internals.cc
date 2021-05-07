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

/** @file pleaf/pleaf_internals.cc
 P-Leaf Internal Interfaces

 Created 4/18/2021 Anonymous
 *******************************************************/

#ifdef J3VM

#include "read0types.h"

#include "include/pleaf_bufpage.h"
#include "include/pleaf_internals.h"

#include "include/ebi_tree_utils.h"
#include "include/ebi_tree.h"

/*
 * PLeafIsVisible
 */
int
PLeafIsVisible(ReadView* snapshot,
    trx_id_t xmin, 
    trx_id_t xmax) 
{
	bool xmin_visible;
	bool xmax_visible;

  xmin_visible = snapshot->changes_visible_simple(xmin);
  xmax_visible = snapshot->changes_visible_simple(xmax);

	if (!xmax_visible)
	{
		if (!xmin_visible)
			return PLEAF_LOOKUP_BACKWARD;
		else
			return PLEAF_LOOKUP_FOUND;
	}
	else
	{
    // TODO
    return PLEAF_LOOKUP_FORWARD;
		if (!xmin_visible)
			return PLEAF_LOOKUP_BACKWARD;
		else
			return PLEAF_LOOKUP_FORWARD;
	}
}

/*
 * PLeafCheckVisibility
 *
 * Visibility check using EBI-tree for now
 */
bool
PLeafCheckVisibility(trx_id_t xmin, trx_id_t xmax) 
{
	return (Sift(xmin, xmax) != nullptr);
}

/*
 * PLeafGetVersionInfo
 *
 * Get xmin and xmax value from version id
 */
//void
//PLeafGetVersionInfo(PLeafVersionId version_id,
//						trx_id_t* xmin, 
//						trx_id_t* xmax)
//{
//	*xmin = PLeafGetXmin(version_id);
//	*xmax = PLeafGetXmax(version_id);
//}

/*
 * PLeafCheckAppendness
 *
 * Appendness check in circular array
 */
bool
PLeafCheckAppendness(int cap, uint16_t head, uint16_t tail) 
{
	return ((head == tail) || (head % cap) != (tail % cap));
}

/*
 * PLeafCheckEmptiness
 *
 * Emptiness check in circular array
 */
bool
PLeafCheckEmptiness(uint16_t head, uint16_t tail)
{
	return (head == tail);
}

#endif
