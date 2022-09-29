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

uint32_t get_unused_page_num(struct pager *pager)
{
	return pager->num_pages;
}

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
		set_node_root(root, true);
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

uint32_t *internal_node_num_keys(void *node)
{
	return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t *internal_node_right_child(void *node)
{
	return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t *internal_node_cell(void *node, uint32_t cell_num)
{
	return node + INTERNAL_NODE_HEADER_SIZE +
		cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t *internal_node_child(void *node, uint32_t child_num)
{
	uint32_t num_keys = *internal_node_num_keys(node);

	if (child_num > num_keys) {
		printf("Tried to access child_num %d > num_keys %d\n",
				child_num, num_keys);
		exit(EXIT_FAILURE);
	}

	if (child_num == num_keys)
		return internal_node_right_child(node);

	return internal_node_cell(node, child_num);
}

uint32_t *internal_node_key(void *node, uint32_t key_num)
{
	return (void *) internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t get_node_max_key(void *node)
{
	switch (get_node_type(node)) {
	case NODE_INTERNAL:
		return *internal_node_key(node,
				*internal_node_num_keys(node) - 1);
	case NODE_LEAF:
		return *leaf_node_key(node,
				*leaf_node_num_cells(node) - 1);
	}

	exit(EXIT_FAILURE);
}

uint32_t *node_parent(void *node)
{
	return node + PARENT_POINTER_OFFSET;
}

bool is_node_root(void *node)
{
	uint8_t value = *((uint8_t *) (node + IS_ROOT_OFFSET));

        return (bool) value;
}

void set_node_root(void *node, bool is_root)
{
	uint8_t value = is_root;

	*((uint8_t *) (node + IS_ROOT_OFFSET)) = value;
}

void create_new_root(struct table *table, uint32_t right_child_page_num)
{
	void *right_child;
	void *left_child;
	void *root;

	uint32_t left_child_page_num;
	uint32_t left_child_max_key;

	root = get_page(table->pager, table->root_page_num);
	right_child = get_page(table->pager, right_child_page_num);
	left_child_page_num = get_unused_page_num(table->pager);
	left_child = get_page(table->pager, left_child_page_num);

	/* left child has data copied from old root */
        memcpy(left_child, root, PAGE_SIZE);
	set_node_root(left_child, false);

	initialize_internal_node(root);
	set_node_root(root, true);
	*internal_node_num_keys(root) = 1;
	*internal_node_child(root, 0) = left_child_page_num;
	left_child_max_key = get_node_max_key(left_child);
	*internal_node_key(root, 0) = left_child_max_key;
	*internal_node_right_child(root) = right_child_page_num;
	*node_parent(left_child) = table->root_page_num;
	*node_parent(right_child) = table->root_page_num;
}

void update_internal_node_key(void *node, uint32_t old_key, uint32_t new_key)
{
	uint32_t old_child_index = internal_node_find_child(node, old_key);
	*internal_node_key(node, old_child_index) = new_key;
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

uint32_t *leaf_node_next_leaf(void *node)
{
	return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void initialize_leaf_node(void *node)
{
	set_node_type(node, NODE_LEAF);
	set_node_root(node, false);
	*leaf_node_num_cells(node) = 0;
	*leaf_node_next_leaf(node) = 0; /* 0 represents no sibbling */
}

void initialize_internal_node(void *node)
{
	set_node_type(node, NODE_INTERNAL);
	set_node_root(node, false);
	*internal_node_num_keys(node) = 0;
}

void leaf_node_split_and_insert(struct cursor *cursor, uint32_t key,
		struct row *value)
{
	uint32_t new_page_num;
	uint32_t old_max;
	void *old_node;
	void *new_node;

	old_node = get_page(cursor->table->pager, cursor->page_num);
	old_max = get_node_max_key(old_node);
	new_page_num = get_unused_page_num(cursor->table->pager);
	new_node = get_page(cursor->table->pager, new_page_num);
	initialize_leaf_node(new_node);
	*node_parent(new_node) = *node_parent(old_node);
	*leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
	*leaf_node_next_leaf(old_node) = new_page_num;

	for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
		uint32_t index_within_node;
		void *dst_node;
		void *dst;

		if (i >= LEAF_NODE_LEFT_SPLIT_COUNT)
			dst_node = new_node;
		else
			dst_node = old_node;

		index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
		dst = leaf_node_cell(dst_node, index_within_node);

		if (i == cursor->cell_num) {
			serialize_row(value, leaf_node_value(dst_node, index_within_node));
			*leaf_node_key(dst_node, index_within_node) = key;
		} else if (i > cursor->cell_num) {
			memcpy(dst, leaf_node_cell(old_node, i - 1),
					LEAF_NODE_CELL_SIZE);
		} else {
			memcpy(dst, leaf_node_cell(old_node, i),
					LEAF_NODE_CELL_SIZE);
		}
	}

	*leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
	*leaf_node_num_cells(new_node) = LEAF_NODE_RIGHT_SPLIT_COUNT;

	if (is_node_root(old_node)) {
		create_new_root(cursor->table, new_page_num);
		return;
	} else {
		uint32_t parent_page_num = *node_parent(old_node);
		uint32_t new_max = get_node_max_key(old_node);
		void *parent = get_page(cursor->table->pager, parent_page_num);

		update_internal_node_key(parent, old_max, new_max);
		internal_node_insert(cursor->table, parent_page_num, new_page_num);
		return;
	}
}

void leaf_node_insert(struct cursor *cursor, uint32_t key, struct row *value)
{
	void *node = get_page(cursor->table->pager, cursor->page_num);
	uint32_t num_cells;

	num_cells = *leaf_node_num_cells(node);
	if (num_cells >= LEAF_NODE_MAX_CELLS) {
		leaf_node_split_and_insert(cursor, key, value);
		return;
	}

	if (cursor->cell_num < num_cells) {
		for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
			void *src = leaf_node_cell(node, i);
			void *dst = leaf_node_cell(node, i - 1);

                        memcpy(src, dst, LEAF_NODE_CELL_SIZE);
		}
	}

	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor->cell_num)) = key;

	serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

uint32_t internal_node_find_child(void *node, uint32_t key)
{
	/* return the index of the child which should contain the given key */
	uint32_t max_index;
	uint32_t min_index;
	uint32_t num_keys;

	num_keys = *internal_node_num_keys(node);

	/* Binary search */
	min_index = 0;
	max_index = num_keys; /* there is one more child than key */

	while (min_index != max_index) {
		uint32_t index = (min_index + max_index) / 2;
		uint32_t key_to_right = *internal_node_key(node, index);

		if (key_to_right >= key)
			max_index = index;
		else
			min_index = index + 1;
	}

	return min_index;
}

struct cursor *internal_node_find(struct table *table, uint32_t page_num, uint32_t key)
{
	void *node = get_page(table->pager, page_num);
	uint32_t child_index = internal_node_find_child(node, key);
	uint32_t child_num = *internal_node_child(node, child_index);
	void *child = get_page(table->pager, child_num);

	switch (get_node_type(child)) {
	case NODE_LEAF:
		return leaf_node_find(table, child_num, key);
	case NODE_INTERNAL:
		return internal_node_find(table, child_num, key);
	}

	return NULL;
}

void internal_node_insert(struct table *table, uint32_t parent_page_num,
		uint32_t child_page_num)
{
	uint32_t right_child_page_num;
	void *right_child;

	/* add a new child/key pair to parent that corresponds to child */
	void *parent = get_page(table->pager, parent_page_num);
	void *child = get_page(table->pager, child_page_num);
	uint32_t child_max_key = get_node_max_key(child);
	uint32_t index = internal_node_find_child(parent, child_max_key);

	uint32_t original_num_keys = *internal_node_num_keys(parent);
	*internal_node_num_keys(parent) = original_num_keys + 1;

	if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
		printf("Need to implement splitting internal node\n");
		exit(EXIT_FAILURE);
	}

	right_child_page_num = *internal_node_right_child(parent);
	right_child = get_page(table->pager, right_child_page_num);

	if (child_max_key > get_node_max_key(right_child)) {
		/* replace child */
		*internal_node_child(parent, original_num_keys) =
			right_child_page_num;
		*internal_node_key(parent, original_num_keys) =
			get_node_max_key(right_child);
		*internal_node_right_child(parent) = child_page_num;
	} else {
		/* make room for the new cell */
		for (uint32_t i = original_num_keys; i > index; i--) {
			void *dst = internal_node_cell(parent, i);
			void *src = internal_node_cell(parent, i - 1);
			memcpy(dst, src, INTERNAL_NODE_CELL_SIZE);
		}

		*internal_node_child(parent, index) = child_page_num;
		*internal_node_key(parent, index) = child_max_key;
	}
}

struct cursor *leaf_node_find(struct table *table, uint32_t page_num,
		uint32_t key)
{
	struct cursor *cursor;

        uint32_t one_past_max_index;
	uint32_t num_cells;
	uint32_t min_index;

        void *node;

        node = get_page(table->pager, page_num);
	num_cells = *leaf_node_num_cells(node);

	one_past_max_index = num_cells;
	min_index = 0;

	cursor = malloc(sizeof(*cursor));
	cursor->page_num = page_num;
	cursor->table = table;

	while (one_past_max_index != min_index) {
		uint32_t key_at_index;
		uint32_t index;

		index = (min_index + one_past_max_index) / 2;
		key_at_index = *leaf_node_key(node, index);

		if (key == key_at_index) {
			cursor->cell_num = index;
			return cursor;
		}

                if (key < key_at_index) {
			one_past_max_index = index;
		} else {
			min_index = index + 1;
		}
	}

	cursor->cell_num = min_index;

        return cursor;
}

enum node_type get_node_type(void *node)
{
	uint8_t value = *((uint8_t *) (node + NODE_TYPE_OFFSET));

	return (enum node_type) value;
}

void set_node_type(void *node, enum node_type type)
{
	uint8_t value = type;

	*((uint8_t *) (node + NODE_TYPE_OFFSET)) = value;
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

void indent(uint32_t level)
{
	printf("%*s", level, " ");
}

void print_tree(struct pager *pager, uint32_t page_num, uint32_t level)
{
	uint32_t num_keys;
	uint32_t child;

        void *node;

	node = get_page(pager, page_num);

	switch (get_node_type(node)) {
	case NODE_LEAF:
		num_keys = *leaf_node_num_cells(node);
		indent(level);
		printf("- leaf (size %d)\n", num_keys);

		for (uint32_t i = 0; i < num_keys; i++) {
			indent(level + 1);
			printf("- %d\n", *leaf_node_key(node, i));
		}
		break;

	case NODE_INTERNAL:
		num_keys = *internal_node_num_keys(node);
		indent(level);
		printf("- internal (size %d)\n", num_keys);

		for (uint32_t i = 0; i < num_keys; i++) {
			child = *internal_node_child(node, i);
			print_tree(pager, child, level + 1);

			indent(level + 1);
			printf("- key %d\n", *internal_node_key(node, i));
		}

		child = *internal_node_right_child(node);
		print_tree(pager, child, level + 1);
		break;
	}
}
