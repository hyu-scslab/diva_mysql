/*-------------------------------------------------------------------------
 *
 * pleaf_internals.cc
 * 		Implement helper functions used in pleaf_reader / pleaf_writer 
 *
 * 
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA

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
