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

#include <simpledb/compiler.h>

enum meta_command_result do_meta_command(struct input_buffer *input)
{
	if (strncmp(input->buffer, ".exit", input->input_length) == 0)
		exit(EXIT_SUCCESS);

        return META_COMMAND_UNRECOGNIZED_COMMAND;
}

enum prepare_result prepare_statement(struct input_buffer *input,
		struct statement *statement)
{
	if (strncmp(input->buffer, "insert", 6) == 0) {
		statement->type = STATEMENT_INSERT;
		return PREPARE_SUCCESS;
	}

	if (strncmp(input->buffer, "select", input->input_length) == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(struct statement *statement)
{
	switch (statement->type) {
	case STATEMENT_INSERT:
		printf("TODO: implement insert.\n");
		break;
	case STATEMENT_SELECT:
		printf("TODO: implement select.\n");
		break;
	}
}
