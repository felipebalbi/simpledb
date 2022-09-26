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

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cursor.h"
#include "db.h"

struct cursor *table_start(struct table *table)
{
	struct cursor *cursor;
	uint32_t num_cells;
	void *root;

	cursor = malloc(sizeof(*cursor));
	if (!cursor) {
		fprintf(stderr, "Failed to create cursor\n");
		exit(EXIT_FAILURE);
	}

	cursor->table = table;
	cursor->page_num = table->root_page_num;
	cursor->cell_num = 0;

	root = get_page(table->pager, table->root_page_num);
	num_cells = *leaf_node_num_cells(root);
	cursor->end = !num_cells;

	return cursor;
}

struct cursor *table_find(struct table *table, uint32_t key)
{
	uint32_t root_page_num = table->root_page_num;
	void *root_node = get_page(table->pager, root_page_num);

	if (get_node_type(root_node) == NODE_LEAF) {
		return leaf_node_find(table, root_page_num, key);
	} else {
		return internal_node_find(table, root_page_num, key);
	}
}

void *cursor_value(struct cursor *cursor)
{
	uint32_t page_num = cursor->page_num;
	void *page = get_page(cursor->table->pager, page_num);

	return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(struct cursor *cursor)
{
	uint32_t page_num = cursor->page_num;
	void *node = get_page(cursor->table->pager, page_num);

        cursor->cell_num += 1;
        if (cursor->cell_num >= (*leaf_node_num_cells(node)))
		cursor->end = true;
}
