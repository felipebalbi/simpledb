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

#include "buffer.h"

enum meta_command_result {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND,
};

enum prepare_result {
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT,
};

enum statement_type {
	STATEMENT_INSERT,
	STATEMENT_SELECT,
};

struct statement {
	enum statement_type type;
};

enum meta_command_result do_meta_command(struct input_buffer *input);
enum prepare_result prepare_statement(struct input_buffer *input,
		struct statement *statement);
void execute_statement(struct statement *statement);

#endif /* __COMPILER_H__ */
