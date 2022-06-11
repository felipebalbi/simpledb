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

static pid_t start_child(int rpipes[2], int wpipes[2])
{
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
		exit(EXIT_FAILURE);
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
	}

	return child;
}

static void send_cmd(int pipe, char *cmd)
{
	write(pipe, cmd, strlen(cmd));
}

static void recv_response(int pipe, char *output, size_t len)
{
	read(pipe, output, len);
}

static void stop_child(int pipe)
{
	send_cmd(pipe, ".exit");
}

static void run_script(char **cmds, char *output, size_t len)
{
	int rpipes[2];
	int wpipes[2];
        pid_t child;

	child = start_child(rpipes, wpipes);

	if (child > 0) { /* parent */
		close(wpipes[0]);
		close(rpipes[1]);

		while (*cmds) {
			send_cmd(wpipes[1], *cmds);
			cmds++;
		}

		stop_child(wpipes[1]);
		recv_response(rpipes[0], output, len);
	}
}

Test(database, simply_exits)
{
	char output[OUTPUT_MAX];

	char *cmds[] = {
		".exit\n",
		NULL
	};

	memset(output, 0x00, OUTPUT_MAX);
	run_script(cmds, output, OUTPUT_MAX);
	cr_assert(eq(str, output, "simpledb > "));
}

Test(database, select_and_exit)
{
	char output[OUTPUT_MAX];

	char *cmds[] = {
		"select\n"
		".exit\n",
		NULL
	};

	memset(output, 0x00, OUTPUT_MAX);
	run_script(cmds, output, OUTPUT_MAX);
	cr_assert(eq(str, output, "simpledb > "
					"Executed.\n"
					"simpledb > "));
}

Test(database, handles_unknown_command)
{
	char output[OUTPUT_MAX];

	char *cmds[] = {
		".foo\n"
		".exit\n",
		NULL
	};

	memset(output, 0x00, OUTPUT_MAX);
	run_script(cmds, output, OUTPUT_MAX);
	cr_assert(eq(str, output, "simpledb > "
					"Unrecognized command '.foo'\n"
					"simpledb > "));
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

	memset(output, 0x00, OUTPUT_MAX);
	run_script(cmds, output, OUTPUT_MAX);
	cr_assert(eq(str, output, "simpledb > "
					"Executed.\n"
					"simpledb > "
					"(1, user1, person1@example.com)\n"
					"Executed.\n"
					"simpledb > "));
}

#if 0
Test(database, prints_error_when_table_full)
{
	int rpipes[2];
	int wpipes[2];
	char output[OUTPUT_MAX];
	char cmd[64];
	pid_t child;

	child = start_child(rpipes, wpipes);
	close(wpipes[0]);
	close(rpipes[1]);
        
        for (int i = 0; i < 1402; i++) {
		sprintf(cmd, "insert %d user%d person%d@example.com\n",
				i, i, i);
		send_cmd(wpipes[1], cmd);
		recv_response(rpipes[0], output, OUTPUT_MAX);
	}
		
	stop_child(wpipes[1]);
	recv_response(rpipes[0], output, OUTPUT_MAX);
	cr_assert(eq(str, output, "simpledb > Error: Table full."));
}
#endif
