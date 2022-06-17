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

struct cursor;

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

struct pager {
	int fd;
	uint32_t len;
	uint32_t num_pages;
	void *pages[TABLE_MAX_PAGES];
};

struct table {
	struct pager *pager;
	uint32_t root_page_num;
};

enum node_type {
	NODE_INTERNAL,
	NODE_LEAF,
};

/* Common Node Header Layout */
#define NODE_TYPE_SIZE		(sizeof(uint8_t))
#define IS_ROOT_SIZE		(sizeof(uint8_t))
#define PARENT_POINTER_SIZE	(sizeof(uint32_t))
#define COMMON_NODE_HEADER_SIZE	(NODE_TYPE_SIZE + IS_ROOT_SIZE + \
			PARENT_POINTER_SIZE)

#define NODE_TYPE_OFFSET	(0)
#define IS_ROOT_OFFSET		(NODE_TYPE_SIZE)
#define PARENT_POINTER_OFFSET	(IS_ROOT_OFFSET + IS_ROOT_SIZE)

/* Leaf Node Header Layout */
#define LEAF_NODE_NUM_CELLS_SIZE (sizeof(uint32_t))
#define LEAF_NODE_NUM_CELLS_OFFSET (COMMON_NODE_HEADER_SIZE)
#define LEAF_NODE_HEADER_SIZE	(COMMON_NODE_HEADER_SIZE + \
			LEAF_NODE_NUM_CELLS_SIZE)

/* Leaf Node Body Layout */
#define LEAF_NODE_KEY_SIZE	(sizeof(uint32_t))
#define LEAF_NODE_VALUE_SIZE	(ROW_SIZE)
#define LEAF_NODE_CELL_SIZE	(LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)

#define LEAF_NODE_KEY_OFFSET	(0)
#define LEAF_NODE_VALUE_OFFSET	(LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)

#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS	(LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

void *get_page(struct pager *pager, uint32_t page_num);
struct pager *pager_open(const char *filename);
void serialize_row(struct row *src, void *dst);
void deserialize_row(void *src, struct row *dst);
struct table *db_open(const char *filename);
void db_close(struct table *table);

uint32_t *leaf_node_num_cells(void *node);
void *leaf_node_cell(void *node, uint32_t cell);
uint32_t *leaf_node_key(void *node, uint32_t cell);
void *leaf_node_value(void *node, uint32_t cell);
void initialize_leaf_node(void *node);
void leaf_node_insert(struct cursor *cursor, uint32_t key, struct row *value);

void print_row(struct row *row);
void print_constants(void);
void printf_leaf_node(void *node);

#endif /* __DB_H__ */
