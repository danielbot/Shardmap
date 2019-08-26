/*
 * Copyright (c) Daniel Phillips, 2002-2017
 * License for distribution granted under the terms of the GPL Version 3
 * The original author reserves the right to dual license this work
 * These lines must be preserved as is in any derivative of this work
 */

#include <stdio.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include "options.h"

static struct optv *optstart(void *work, unsigned size)
{
	struct optv *optv = (struct optv *)work;
	*optv = (struct optv){ .size = size };
	return optv;
}

static int is_number(const char *str)
{
	const unsigned char *num = (const unsigned char *)str;
	unsigned c;

	while ((c = *num++)) {
		if (c - '0' >= 10)
			return 0;
	}

	return 1;
}

static int optparse(struct option *options, struct optv *optv, const char **argv, int argc, int *pos)
{
	struct opt *top = (struct opt *)((char *)optv + optv->size);
	const char *arg, *why;
	char *errout = (char *)optv->argv;
	int fake = !optv->size, terse = 0, longlen = 0, rule;
	int free = (char *)(top - optv->optc) - (char *)(optv->argv + optv->argc);
	int maxerr = fake ? 0 : (char *)top - errout;
	struct option *option;

	optv->err = 0;

	arg = argv[(*pos)++];
	if (options && *arg == '-' && *(arg + 1)) {
		const char *val = NULL;
		if (*++arg == '-') {
			if (!*++arg)
				return 1;
			longlen = (val = strchr(arg, '=')) ? val++ - arg : strlen(arg);
			terse = 0;
		} else
			terse = *arg++;

		do {
			for (option = options; option->name; option++)
				if (terse ? !!strchr(option->terse, terse) : !memcmp(option->name, arg, longlen))
					break;
			if (!option->name)
				goto name;
			if (terse && (option->rule & OPT_ANYARG) && *arg)
				val = arg;
			else if ((option->rule & OPT_ANYARG) == OPT_HASARG && !val) {
				why = "must have a value";
				if (*pos >= argc)
					goto fail;
				val = argv[(*pos)++];
			}
			optv->optc++;
			if (!(rule = option->rule) && val) {
				why = "must not have a value";
				goto fail;
			}
			if (!fake) {
				if ((free -= sizeof(struct opt)) < 0)
					goto full;
				if (!(rule & OPT_MANY)) {
					struct opt *seen = top;
					why = "used more than once";
					while (--seen > top - optv->optc)
						if (option - options == seen->index)
							goto fail;
				}
				if (rule & OPT_NUMBER) {
					if (!is_number(val)) {
						why = "must be a number";
						goto fail;
					}
				}
				*(top - optv->optc) = (struct opt){(int)(option - options), val ? : option->defarg};
			}
		} while (terse && !(rule & ~OPT_MANY) && (terse = *arg++));

		return 0;
	}
	if (!fake) {
		if ((free -= sizeof(arg)) < 0)
			goto full;
		optv->argv[optv->argc] = arg;
	}
	optv->argc++;

	return 0;

name:
	if (terse)
		snprintf(errout, maxerr, "Unknown option -%c", terse);
	else
		snprintf(errout, maxerr, "Unknown option --%.*s", longlen, arg);
	return optv->err = -EINVAL;

fail:
	if (terse)
		snprintf(errout, maxerr, "-%c option (%s) %s", terse, option->name, why);
	else
		snprintf(errout, maxerr, "--%s option %s", option->name, why);
	return optv->err = -EINVAL;

full:
	snprintf((char *)optv->argv, maxerr, "Out of space in optv");
	return optv->err = -E2BIG;
}

int opthead(struct option *options, int *argc, const char ***argv, void *work, int size, unsigned stop)
{
	struct optv *optv = optstart(work, size);
	int pos = 0;

	while (pos < *argc) {
		int err = optparse(options, optv, *argv, *argc, &pos);
		if (err) {
			if (err < 0)
				return err;
			options = NULL;
		}
		if (stop && optv->argc >= stop) {
			while (pos < *argc)
				optv->argv[optv->argc++] = (*argv)[pos++];
			break;
		}
	}
	if (optv->size) {
		*argc = optv->argc;
		*argv = optv->argv;
	}

	return optv->optc;
}

int optscan(struct option *options, int *argc, const char ***argv, void *work, int size)
{
	return opthead(options, argc, argv, work, size, 0);
}

int optspace(struct option *options, int argc, const char *argv[])
{
	struct optv fake = {};
	optscan(options, &argc, &argv, &fake, 0);
	int size = sizeof(fake) + fake.argc * sizeof(char *) + fake.optc * sizeof(struct opt);
	return fake.err ? 100 : size;
}

int optcount(char *work, unsigned opt)
{
	int count = 0;
	for (unsigned i = 0; i < ((struct optv *)work)->optc; i++)
		count += optindex(work, i) == opt;
	return count;
}

const char *opterror(char *work)
{
	struct optv *optv = (struct optv *)work;
	return optv->err ? (const char *)optv->argv : NULL;
}

/* Generate help text */

struct emit {
	char *text;
	int full, size, over;
};

static int emit(struct emit *text, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	int room = text->full - text->size, over = 0;
	int size = vsnprintf(text->text + text->size, room, fmt, args);
	va_end(args);
	if (size > room) {
		text->over += over = size - room;
		size = room;
	}
	text->size += size;
	return over;
}

static int emitpad(struct emit *text, int pad)
{
	return emit(text, "%*s", pad, "");
}

static int emitend(struct emit *text)
{
	return emit(text, "\n");
}

static int emitrule(struct emit *text, struct option *option)
{
	if ((option->rule & 3)) {
		const char *type = option->arghelp ? : (option->rule & OPT_NUMBER) ? "number" : "value";
		int optional = (option->rule & 3) == OPT_OPTARG;
		if (optional)
			emit(text, "[=%s]", type);
		else
			emit(text, "=%s", type);
	}
	return 0;
}

int opthelp(char *buf, int bufsize, struct option *options, int tabs[3], char *lead, int brief)
{
	int tab0 = tabs ? tabs[0] : 3;
	int tab1 = tabs ? tabs[1] : 30;
	int tab2 = tabs ? tabs[2] : 80;
	struct emit emittext = {
		.text	= buf,
		.full	= bufsize,
	};
	struct emit *text = &emittext;
	struct option *option;

	emit(text, "%s%s", lead, brief ? " " : lead[0] ? "\n" : "");

	int i, left = brief ? 0 : text->size;
	for (option = options; option->name; option++) {
		const char *terse = option->terse;
		if (brief) {
			for (i = 0; i < 2; i++) {
				int mark = text->size, over = text->over;
				emit(text, "[");
				while (*terse) {
					unsigned char c = *terse++;
					if (c > ' ') // !iscntrl
						emit(text, "-%c|", c);
				}
				emit(text, "--%s", option->name);
				emitrule(text, option);
				emit(text, "] ");
				if (text->size - left < tab2)
					break;
				text->size = mark;
				text->over = over;
				emitend(text);
				left = text->size;
				emitpad(text, tab0);
			}
			continue;
		}

		emitpad(text, tab0);
		emit(text, "--%s", option->name);
		emitrule(text, option);
		while (*terse) {
			unsigned char c = *terse++;
			if (c > ' ') // !iscntrl
				emit(text, ", -%c", c);
		}
		if (option->help) {
			int col = text->size + text->over - left, pad = tab1 > col ? tab1 - col : 0;
			emit(text, "%*s", pad, " ");
			const char *help = option->help;
			int tail = strlen(help);
			while (1) {
				char *top = text->text + text->full;
				col = text->size + text->over - left;
				int room = tab2 > col ? tab2 - col : 0;
				int size = tail < room ? tail : room;
				int free = top - (text->text + text->size);
				int mark = text->size, over = text->over;
				if (size > free) {
					text->over += size - free;
					size = free;
				}
				if (text->size == text->full)
					break;
				memcpy(text->text + text->size, help, size);
				text->size += size;
				if (tail <= size)
					break;
				text->size = mark;
				text->over = over;
				int wrap = size, most = tab2 - tab1 - 1;
				if (most > 10)
					most = 10;
				while (wrap > size - most)
					if (help[wrap--] == ' ') {
						size = wrap + 2;
						text->size--;
						break;
					}
				text->size += size;
				help += size;
				tail -= size;
				emitend(text);
				left = text->size + text->over;
				emitpad(text, tab1);
			}
		}
		emitend(text);
		left = text->size + text->over;
	}
	return -text->over;
}

#if 0
/*
 * Copyright (c) Daniel Phillips, 2002-2017
 * License for distribution granted under the terms of the GPL Version 3
 * The original author reserves the right to dual license this work
 * These lines must be preserved as is in any derivative of this work
 *
 * gcc -std=gnu99 -Wall test.c options.c -o test && ./test a b c -e test
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/ioctl.h> // just for terminal size awareness in help/usage
#include "options.h"

void show_version(void)
{
	printf("Example V%s\n", "0.0");
}

void usage(struct option *options, const char *name, const char *blurb)
{
	const char *usage = "";
	struct winsize winsize;
	int cols = ioctl(1, TIOCGWINSZ, &winsize) ? 80 : winsize.ws_col;
	int tabs[] = {3, 40, cols < 60 ? 60 : cols};
	char lead[300], help[3000] = {};
	snprintf(lead, sizeof(lead), "Usage: %s%s%s", name, blurb ? : "", usage);
	opthelp(help, sizeof(help), options, tabs, lead, !blurb);
	printf("%s\n", help);
}

int main(int argc, char *argv[])
{
	struct option options[] = {
		{"example", "e", OPT_HASARG, "Example option with argument", "string"},
		{"version", "V", 0, "Show version"},
		{"usage", "", 0, "Show usage"},
		{"help", "?", 0, "Show help"},
		{}};

	char optv[1000];
	int optc = optscan(options, &argc, (const char ***)&argv, optv, sizeof(optv));
	char *blurb = "options-example [options] <parameters>";

	if (optc < 0) {
	        printf("%s!\n", opterror(optv));
		exit(1);
	}

	for (int i = 0; i < optc; i++) {
		struct option *option = options + optindex(optv, i);
		switch (option->terse[0]) {
		case 'e':
			printf("Example value: '%s'\n", optvalue(optv, i));
			break;
		case 'V':
			show_version();
			exit(0);
		case '?':
			usage(options, blurb, " [OPTIONS]");
			exit(0);
		case 0:
			usage(options, blurb, 0);
			exit(0);
		}
	}

	printf("Arguments: ");
	for (int i = 0; i < argc; i++)
		printf("'%s' ", argv[i]);
	printf("\n");
}
#endif
