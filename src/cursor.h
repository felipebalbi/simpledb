/* SPDX-License-Identifier: GPL-3.0-or-later */

/*
 * This file is part of simpledb
 *
 * simpledb is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * simpledb is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with simpledb.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __CURSOR_H__
#define __CURSOR_H__

#include "db.h"

struct cursor {
	struct table *table;
	uint32_t page_num;
	uint32_t cell_num;
	bool end;
};

struct cursor *table_start(struct table *table);
struct cursor *table_end(struct table *table);
void *cursor_value(struct cursor *cursor);
void cursor_advance(struct cursor *cursor);

#endif /* __CURSOR_H__ */
