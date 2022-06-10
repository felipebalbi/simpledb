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

#include <criterion/criterion.h>
#include <criterion/new/assert.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

#define OUTPUT_MAX 4096

static void run_script(char **cmds, char *output, size_t len)
{
	int rpipes[2];
	int wpipes[2];
        pid_t child;
	int ret;
	
	ret = pipe(rpipes);
	if (ret != 0) {
		fprintf(stderr, "Failed to create read pipes\n");
		exit(EXIT_FAILURE);
	}

        ret = pipe(wpipes);
	if (ret != 0) {
		fprintf(stderr, "Failed to create write pipes\n");
		close(rpipes[0]);
		close(rpipes[1]);
	}

	child = fork();
	if (child == -1) {
		fprintf(stderr, "Failed to fork.\n");
		exit(EXIT_FAILURE);
	}

        if (child == 0) { /* child */
		char *exe[] = { "./simpledb", NULL };

		close(wpipes[1]);
		close(rpipes[0]);

                dup2(wpipes[0], STDIN_FILENO);
		dup2(rpipes[1], STDOUT_FILENO);

                ret = execv(exe[0], exe);
		if (ret == -1) {
			fprintf(stderr, "Failed to execute. %s\n",
					strerror(errno));
		}
	} else { /* parent */
		close(wpipes[0]);
		close(rpipes[1]);

		while (*cmds) {
			write(wpipes[1], *cmds, strlen(*cmds));
			cmds++;
		}

                wait(NULL);
		read(rpipes[0], output, len);
	}
}

Test(database, inserts_and_retrieves_row)
{
	char output[OUTPUT_MAX];

	char *cmds[] = {
		"insert 1 user1 person1@example.com\n",
		"select\n",
		".exit\n",
		NULL
	};

	run_script(cmds, output, OUTPUT_MAX);
	cr_assert(eq(str, output, "simpledb > Executed.\n\
simpledb > (1, user1, person1@example.com)\n\
Executed.\nsimpledb > "));
}
