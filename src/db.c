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

#include "buffer.h"
#include "compiler.h"
#include "db.h"
#include "cursor.h"

void *get_page(struct pager *pager, uint32_t page_num)
{
	if (page_num > TABLE_MAX_PAGES) {
		fprintf(stderr, "Page number out of bounds --> %d > %d",
				page_num, TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}

	if (pager->pages[page_num] == NULL) {
		/* Cache miss: allocate memory and load from file. */
		void *page = malloc(PAGE_SIZE);
		uint32_t num_pages = pager->len / PAGE_SIZE;

		/* We might save a partial page at the end of the file */
		if (pager->len % PAGE_SIZE)
			num_pages += 1;

		if (page_num <= num_pages) {
			ssize_t bytes;

                        lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
			bytes = read(pager->fd, page, PAGE_SIZE);
			if (bytes < 0) {
				fprintf(stderr, "Error reading file: %s\n",
						strerror(errno));
				exit(EXIT_FAILURE);
			}
		}

		pager->pages[page_num] = page;

		if (page_num >= pager->num_pages)
			pager->num_pages = page_num + 1;
	}

	return pager->pages[page_num];
}

static void pager_flush(struct pager *pager, uint32_t page_num)
{
	ssize_t bytes;
	off_t offset;

        if (!pager->pages[page_num]) {
		fprintf(stderr, "Tried to flush NULL page\n");
		exit(EXIT_FAILURE);
	}

	offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
	if (offset < 0) {
		fprintf(stderr, "Error seeking: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	bytes = write(pager->fd, pager->pages[page_num], PAGE_SIZE);
	if (bytes < 0) {
		fprintf(stderr, "Error writing: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}
}

struct pager *pager_open(const char *filename)
{
	struct pager *pager;
	off_t len;
	int fd;

	fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
	if (fd == -1) {
		fprintf(stderr, "Unable to open file %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	len = lseek(fd, 0, SEEK_END);
	pager = malloc(sizeof(*pager));
	pager->fd = fd;
	pager->len = len;
	pager->num_pages = len / PAGE_SIZE;

	if (len % PAGE_SIZE) {
		printf("Db file is not aligned to PAGE_SIZE\n");
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
		pager->pages[i] = NULL;

        return pager;
}

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

struct table *db_open(const char *filename)
{
	struct pager *pager = pager_open(filename);
	struct table *table = malloc(sizeof(*table));

	table->pager = pager;
	table->root_page_num = 0;

	if (!pager->num_pages) {
		void *root = get_page(pager, 0);

		initialize_leaf_node(root);
	}
        
	return table;
}

void db_close(struct table *table)
{
	struct pager *pager = table->pager;
	int ret;

	for (uint32_t i = 0; i < pager->num_pages; i++) {
		if (!pager->pages[i])
			continue;

		pager_flush(pager, i);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	ret = close(pager->fd);
	if (ret < 0) {
		fprintf(stderr, "Error closing db file: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		void *page = pager->pages[i];

		free(page);
		pager->pages[i] = NULL;
	}

        free(pager);
	free(table);
}

uint32_t *leaf_node_num_cells(void *node)
{
	return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void *node, uint32_t cell)
{
	return node + LEAF_NODE_HEADER_SIZE + cell * LEAF_NODE_CELL_SIZE;
}

uint32_t *leaf_node_key(void *node, uint32_t cell)
{
	return leaf_node_cell(node, cell);
}

void *leaf_node_value(void *node, uint32_t cell)
{
	return leaf_node_cell(node, cell) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node)
{
	*leaf_node_num_cells(node) = 0;
}

void leaf_node_insert(struct cursor *cursor, uint32_t key, struct row *value)
{
	void *node = get_page(cursor->table->pager, cursor->page_num);
	uint32_t num_cells;

	num_cells = *leaf_node_num_cells(node);
	if (num_cells >= LEAF_NODE_MAX_CELLS) {
		printf("Need to implement splitting a leaf node\n");
		exit(EXIT_FAILURE);
	}

	if (cursor->cell_num < num_cells) {
		for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
			void *src = leaf_node_cell(node, i - 1);
			void *dst = leaf_node_cell(node, i);

                        memcpy(src, dst, LEAF_NODE_CELL_SIZE);
		}
	}

	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor->cell_num)) = key;

	serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void print_row(struct row *row)
{
	printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void print_constants(void)
{
	printf("%25s: %5lu\n", "ROW_SIZE",
			ROW_SIZE);
	printf("%25s: %5lu\n", "COMMON_NODE_HEADER_SIZE",
			COMMON_NODE_HEADER_SIZE);
	printf("%25s: %5lu\n", "LEAF_NODE_HEADER_SIZE",
			LEAF_NODE_HEADER_SIZE);
	printf("%25s: %5lu\n", "LEAF_NODE_CELL_SIZE",
			LEAF_NODE_CELL_SIZE);
	printf("%25s: %5lu\n", "LEAF_NODE_SPACE_FOR_CELLS",
			LEAF_NODE_SPACE_FOR_CELLS);
	printf("%25s: %5lu\n", "LEAF_NODE_MAX_CELLS",
			LEAF_NODE_MAX_CELLS);
}

void printf_leaf_node(void *node)
{
	uint32_t num_cells = *leaf_node_num_cells(node);

	printf("leaf (size %d)\n", num_cells);

	for (uint32_t i = 0; i < num_cells; i++) {
		uint32_t key;

		key = *leaf_node_key(node, i);
		printf("    - %u : %u\n", i, key);
	}
}
