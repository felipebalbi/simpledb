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

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "compiler.h"
#include "table.h"

void serialize_row(struct row *src, void *dst)
{
	memcpy(dst + ID_OFFSET, &src->id, ID_SIZE);
	memcpy(dst + USERNAME_OFFSET, &src->username, USERNAME_SIZE);
	memcpy(dst + EMAIL_OFFSET, &src->email, EMAIL_SIZE);
}

void deserialize_row(void *src, struct row *dst)
{
	memcpy(&dst->id, src + ID_OFFSET, ID_SIZE);
	memcpy(&dst->username, src + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&dst->email, src + EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot(struct table *table, uint32_t row_num)
{
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void *page = table->pages[page_num];
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;

	/* Allocate memory only when accessing a page */
	if (!page) {
		page = malloc(PAGE_SIZE);
		table->pages[page_num] = page;
	}

	return page + byte_offset;
}

struct table *new_table(void)
{
	struct table *table = malloc(sizeof(*table));

	table->num_rows = 0;

        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
		table->pages[i] = NULL;

	return table;
}

void free_table(struct table *table)
{
	for (uint32_t i = 0; table->pages[i]; i++)
		free(table->pages[i]);

	free(table);
}

void print_row(struct row *row)
{
	printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}
