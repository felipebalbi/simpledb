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

#include "compiler.h"
#include "cursor.h"
#include "db.h"

enum meta_command_result do_meta_command(struct input_buffer *input,
		struct table *table)
{
	if (strncmp(input->buffer, ".exit", input->input_length) == 0) {
		close_input_buffer(input);
		db_close(table);
		exit(EXIT_SUCCESS);
	} else if (strncmp(input->buffer, ".constants",
					input->input_length) == 0) {
		printf("Constants:\n");
		print_constants();
		return META_COMMAND_SUCCESS;
	} else if (strncmp(input->buffer, ".btree",
					input->input_length) == 0) {
		printf("Tree:\n");
		print_tree(table->pager, 0, 0);
		return META_COMMAND_SUCCESS;
	}

        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

enum prepare_result prepare_insert(struct input_buffer *input,
		struct statement *statement)
{
	int id;

	char *id_string;
	char *username;
	char *email;

	strtok(input->buffer, " ");
	id_string = strtok(NULL, " ");
	username = strtok(NULL, " ");
	email = strtok(NULL, " ");

	if (!id_string || !username || !email)
		return PREPARE_SYNTAX_ERROR;

	id = atoi(id_string);

        if (strlen(username) > COLUMN_USERNAME_SIZE)
		return PREPARE_STRING_TOO_LONG;

	if (strlen(email) > COLUMN_EMAIL_SIZE)
		return PREPARE_STRING_TOO_LONG;

	statement->type = STATEMENT_INSERT;
	statement->row.id = id;
	strcpy(statement->row.username, username);
	strcpy(statement->row.email, email);
        
	return PREPARE_SUCCESS;
}

enum prepare_result prepare_statement(struct input_buffer *input,
		struct statement *statement)
{
	if (strncmp(input->buffer, "insert", 6) == 0) {
		return prepare_insert(input, statement);
	}

	if (strncmp(input->buffer, "select", input->input_length) == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

enum execute_result execute_insert(struct statement *statement,
		struct table *table)
{
	struct cursor *cursor;
	struct row *row;

	uint32_t num_cells;
        uint32_t key;

        void *node;

        node = get_page(table->pager, table->root_page_num);
	num_cells = *leaf_node_num_cells(node);

	row = &statement->row;
	key = row->id;
        cursor = table_find(table, key);

	if (cursor->cell_num < num_cells) {
		uint32_t cur = *leaf_node_key(node, cursor->cell_num);

		if (cur == key)
			return EXECUTE_DUPLICATE_KEY;
	}

	leaf_node_insert(cursor, row->id, row);
        free(cursor);

	return EXECUTE_SUCCESS;
}

enum execute_result execute_select(struct statement *statement,
		struct table *table)
{
	struct cursor *cursor;
	struct row row;

	cursor = table_start(table);

	while (!cursor->end) {
		deserialize_row(cursor_value(cursor), &row);
		print_row(&row);
		cursor_advance(cursor);
	}

	free(cursor);

        return EXECUTE_SUCCESS;
}

enum execute_result execute_statement(struct statement *statement,
		struct table *table)
{
	switch (statement->type) {
	case STATEMENT_INSERT:
		return execute_insert(statement, table);
	case STATEMENT_SELECT:
		return execute_select(statement, table);
	default:
		return EXECUTE_UNKNOWN;
	}
}
