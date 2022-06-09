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

#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

struct input_buffer {
	char *buffer;
	size_t buffer_length;
	ssize_t input_length;
};

struct input_buffer *new_input_buffer(void);
void read_input(struct input_buffer* input);
void close_input_buffer(struct input_buffer *input);

#endif /* __BUFFER_H__ */
