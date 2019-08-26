/*
 * Copyright (c) Daniel Phillips, 2002-2017
 * License for distribution granted under the terms of the GPL Version 3
 * The original author reserves the right to dual license this work
 * These lines must be preserved as is in any derivative of this work
 */

struct optv {
	unsigned size, argc, optc, err;
	const char *argv[];
};

struct option {
	const char *name, *terse;
	unsigned rule;
	const char *help, *arghelp, *defarg;
};

struct opt {
	int index;
	const char *value;
};

enum {
	OPT_NOARG = 0,
	OPT_HASARG = 1,
	OPT_OPTARG = 2,
	OPT_NUMBER = 4,
	OPT_MANY = 8,
	OPT_MAX,

	OPT_ANYARG = OPT_HASARG | OPT_OPTARG,
};

static inline struct opt *optentry(char *work, int i)
{
	struct optv *optv = (struct optv *)work;
	return (struct opt *)(work + optv->size) - i - 1;
}

/* Map ith option found in argv to ith options definition record */
static inline unsigned optindex(char *work, int i)
{
	return optentry(work, i)->index;
}

static inline const char *optvalue(char *work, int i)
{
	return optentry(work, i)->value;
}

static inline struct optv *argv2optv(const char *argv[])
{
	return (struct optv *)((char *)argv - offsetof(struct optv, argv));
}

int opthead(struct option *options, int *argc, const char ***argv, void *work, int size, unsigned stop);
int optscan(struct option *options, int *argc, const char ***argv, void *work, int size);
int optspace(struct option *options, int argc, const char *argv[]);
int optcount(char *work, unsigned opt);
const char *opterror(char *work);
int opthelp(char *buf, int bufsize, struct option *options, int tabs[3], char *lead, int brief);
const char *optbasename(const char *argv0);
