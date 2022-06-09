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

#include <simpledb/buffer.h>
#include <simpledb/compiler.h>

static void print_prompt(void)
{
	printf("simpledb > ");
}

int main(int argc, char* argv[])
{
	struct input_buffer *input = new_input_buffer();

        while (true) {
		struct statement statement;

                print_prompt();
		read_input(input);

		if (input->buffer[0] == '.') {
			switch (do_meta_command(input)) {
			case META_COMMAND_SUCCESS:
				continue;
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				printf("Unrecognized command '%s'\n", input->buffer);
				continue;
			}
		}

		switch (prepare_statement(input, &statement)) {
		case PREPARE_SUCCESS:
			break;
		case PREPARE_UNRECOGNIZED_STATEMENT:
			printf("Unrecognized keyword at start of '%s'.\n",
					input->buffer);
			continue;
		}

		execute_statement(&statement);
		printf("Executed.\n");
	}
}