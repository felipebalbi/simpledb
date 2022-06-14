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

#ifndef __DB_H__
#define __DB_H__

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define COLUMN_USERNAME_SIZE	32
#define COLUMN_EMAIL_SIZE	255

struct row {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
};

#define attr_size(type, member)	(sizeof(((type *) 0)->member))
#define ID_SIZE			(attr_size(struct row, id))
#define USERNAME_SIZE		(attr_size(struct row, username))
#define EMAIL_SIZE		(attr_size(struct row, email))
#define ROW_SIZE		(ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)
#define PAGE_SIZE		4096

#define ID_OFFSET		(0)
#define USERNAME_OFFSET		(ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET		(USERNAME_OFFSET + USERNAME_SIZE)

#define TABLE_MAX_PAGES		100
#define ROWS_PER_PAGE		(PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS		(ROWS_PER_PAGE * TABLE_MAX_PAGES)

struct pager {
	int fd;
	uint32_t len;
	void *pages[TABLE_MAX_PAGES];
};

struct table {
	struct pager *pager;
	uint32_t num_rows;
};

struct pager *pager_open(const char *filename);
void serialize_row(struct row *src, void *dst);
void deserialize_row(void *src, struct row *dst);
void *row_slot(struct table *table, uint32_t row_num);
struct table *db_open(const char *filename);
void db_close(struct table *table);
void print_row(struct row *row);

#endif /* __DB_H__ */
