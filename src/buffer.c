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

struct input_buffer *new_input_buffer()
{
	struct input_buffer *input = malloc(sizeof(*input));

        input->buffer = NULL;
	input->buffer_length = 0;
	input->input_length = 0;

	return input;
}

void read_input(struct input_buffer* input)
{
	ssize_t bytes = getline(&input->buffer, &input->buffer_length, stdin);

	if (bytes <= 0) {
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	/* Ignore trailing newline */
	input->input_length = bytes - 1;
	input->buffer[bytes - 1] = 0;
}

void close_input_buffer(struct input_buffer *input)
{
	free(input->buffer);
	free(input);
}
