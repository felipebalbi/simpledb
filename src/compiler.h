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

#ifndef __COMPILER_H__
#define __COMPILER_H__

#include <stdint.h>
#include "buffer.h"
#include "table.h"

enum meta_command_result {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND,
};

enum prepare_result {
	PREPARE_SUCCESS,
	PREPARE_STRING_TOO_LONG,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT,
};

enum execute_result {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL,
	EXECUTE_UNKNOWN,
};

enum statement_type {
	STATEMENT_INSERT,
	STATEMENT_SELECT,
};

struct statement {
	enum statement_type type;
	struct row row;
};

enum meta_command_result do_meta_command(struct input_buffer *input,
		struct table *table);
enum prepare_result prepare_statement(struct input_buffer *input,
		struct statement *statement);
enum execute_result execute_statement(struct statement *statement,
	struct table *table);

#endif /* __COMPILER_H__ */
