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

static void *get_page(struct pager *pager, uint32_t page_num)
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
	}

	return pager->pages[page_num];
}

static void pager_flush(struct pager *pager, uint32_t page_num, uint32_t size)
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

	bytes = write(pager->fd, pager->pages[page_num], size);
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

void *cursor_value(struct cursor *cursor)
{
	uint32_t row_num = cursor->row_num;
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void *page = get_page(cursor->table->pager, page_num);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;

	return page + byte_offset;
}

void cursor_advance(struct cursor *cursor)
{
	cursor->row_num += 1;
	cursor->end = (cursor->row_num >= cursor->table->num_rows);
}

struct table *db_open(const char *filename)
{
	struct pager *pager = pager_open(filename);
	uint32_t num_rows = pager->len / ROW_SIZE;
	struct table *table = malloc(sizeof(*table));

	table->pager = pager;
	table->num_rows = num_rows;
        
	return table;
}

void db_close(struct table *table)
{
	struct pager *pager = table->pager;
	uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
	uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
	int ret;

	for (uint32_t i = 0; i < num_full_pages; i++) {
		if (!pager->pages[i])
			continue;

		pager_flush(pager, i, PAGE_SIZE);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	/* Handle partial page at the end */
	if (num_additional_rows > 0) {
		uint32_t page_num = num_full_pages;

		if (pager->pages[page_num]) {
			pager_flush(pager, page_num,
					num_additional_rows * ROW_SIZE);
			free(pager->pages[page_num]);
			pager->pages[page_num] = NULL;
		}
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

void print_row(struct row *row)
{
	printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}
