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

struct cursor *table_start(struct table *table)
{
	struct cursor *cursor;

	cursor = malloc(sizeof(*cursor));
	if (!cursor) {
		fprintf(stderr, "Failed to create cursor\n");
		exit(EXIT_FAILURE);
	}

	cursor->table = table;
	cursor->row_num = 0;
	cursor->end = !table->num_rows;

	return cursor;
}

struct cursor *table_end(struct table *table)
{
	struct cursor *cursor;

	cursor = malloc(sizeof(*cursor));
	if (!cursor) {
		fprintf(stderr, "Failed to create cursor\n");
		exit(EXIT_FAILURE);
	}

	cursor->table = table;
	cursor->row_num = table->num_rows;
	cursor->end = true;

	return cursor;
}
