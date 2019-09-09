/*
 * Shardmap fast lightweight key value store
 * (c) 2012 - 2019, Daniel Phillips
 * License: GPL v3
 */

extern "C" {
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include "size.h"
#include "debug.h"
}

#define trace trace_off

#include "recops.cc"
#include "shardmap.h"

#include <type_traits> // is_pod
extern "C" {
#include <sys/ioctl.h> // terminal size awareness in help/usage
#include "options.h"
int uform(char *buf, int len, unsigned long n, unsigned base);
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

int main(int argc, const char *argv[])
{
	if (0) {
		void *p = errwrap(-3), *q = (void *)main, *z = 0;
		printf("%p %p %p\n", p, q, z);
		printf("%i %i %i\n", is_errcode(p), is_errcode(q), is_errcode(z));
		return 0;
	}

	if (1)
		printf("sizeof(shardmap %lu tier %lu shard %lu)\n", sizeof(keymap), sizeof(tier), sizeof(shard));

	if (argc > 1 && !strcmp("tpcb", argv[1])) {
		struct option options[] = {
			{"scale", "s", OPT_HASARG|OPT_NUMBER, "Scale factor", "2"},
			{"nsteps", "n", OPT_HASARG|OPT_NUMBER, "Transaction steps", "1000000"},
			{"version", "V", 0, "Show version"},
			{"usage", "", 0, "Show usage"},
			{"help", "?", 0, "Show help"},
			{}};

		char optv[1000];
		int optc = optscan(options, &argc, (const char ***)&argv, optv, sizeof(optv));
		//const char *blurb = "shardmap tpcb <tablepath>";

		if (optc < 0) {
		        printf("%s!\n", opterror(optv));
			exit(1);
		}

		int s = 2, n = 1000000;

		for (int i = 0; i < optc; i++) {
			struct option *option = options + optindex(optv, i);
			switch (option->terse[0]) {
			case 's':
				s = atoi(optvalue(optv, i));
				trace_off("sf: %i", s);
				break;
			case 'n':
				n = atoi(optvalue(optv, i));
				trace_off("steps: '%i'", n);
				break;
			case 'V':
				printf("Shardmap tpcb benchmark by Daniel Phillips: version 0.0\n");
				exit(0);
			case '?':
				usage(options, argv[0], " tpcb <filename> [OPTIONS]");
				exit(0);
			case 0:
				usage(options, argv[0], 0);
				exit(0);
			}
		}

		if (argc <= 2)
			error_exit(1, "Usage: tcpv <filepath> --sf=<scale> --n=<steps>");

		int fds[5] = {};
		for (int i = 0; i < 5; i++) {
			const std::string path = std::string(argv[2]) + std::to_string(i);
			const char *cpath = path.c_str();
			trace_on("path: %s", cpath);
			if ((fds[i] = open(cpath, O_CREAT|O_RDWR, 0644)) < 0)
				error_exit(1, "could not create %s tables (%s)", cpath, strerror(errno));
			trace("fd %i", fds[i]);
		}

		trace_on("tpcb_run sf %i steps %i", s, n);
		int tpcb_run(int fds[4], unsigned scalefactor, unsigned iterations);
		return !!tpcb_run(fds, s, n);
	}

	if (0) {
		printf("bigmap is_pod %i\n", std::is_pod<bigmap>::value);
		printf("bigmap is_trivially_copyable %i\n", std::is_trivially_copyable<bigmap>::value);
		printf("shardmap is_pod %i\n", std::is_pod<keymap>::value);
		printf("shardmap is_trivially_copyable %i\n", std::is_trivially_copyable<keymap>::value);
		return 0;
	}

	if (argc <= 2)
		error_exit(1, "%s <filename> <iterations>", argv[0]);

	if (0) {
		unsigned n = atoi(argv[2]);
		unsigned bits0 = atoi(argv[1]);
		duopack <u32, u64> duo(bits0);
		u64 sum = 0;

		for (unsigned i = 0; i < n; i++) {
			u32 a;
			u64 b;
			if (1) {
				u64 packed = duo.pack(0x123456, 0x888888);
				hexdump(&packed, sizeof packed);
				duo.unpack(packed, a, b);
				printf("%x %lx\n", a, b);
				printf("%x %lx\n", duo.first(packed), duo.second(packed));
			} else {
				u64 packed = duo.pack(sum, bits0);
				duo.unpack(packed, a, b);
			}
			sum += a ^ b;
		}
		printf("sum %li\n", sum);
		return 0;
	}

	if (0) {
		unsigned n = atoi(argv[3]);
		unsigned bits0 = atoi(argv[1]), bits1 = atoi(argv[2]);
		tripack <u64, u64, u64> test(bits0, bits1);
		u64 sum = 0;

		for (unsigned i = 0; i < n; i++) {
			u64 a, b, c;
			if (1) {
				u64 packed = test.pack(0x9999, 0x888888, 0x123456);
				hexdump(&packed, sizeof packed);
				test.unpack(packed, a, b, c);
				printf("%lx %lx %lx\n", a, b, c);
				printf("%lx %lx %lx\n", test.third(packed), test.second(packed), test.first(packed));
			} else {
				u64 packed = test.pack(bits0, sum, sum);
				test.unpack(packed, a, b, c);
			}
			sum += a ^ b ^ c;
		}
		printf("sum %li\n", sum);
		return 0;
	}

	struct header head = {
		.magic = {'t', 'e', 's', 't'},
		.version = 0,
		.blockbits = 14,
		.tablebits = 9,
		.maxtablebits = 19,
		.reshard = 1, // power of 2
		.rehash = 2, // power of 2
		.loadfactor = one_fixed8,
		.blocks = 0, // is this right???

		.upper = {
			.mapbits = 0,
			.stridebits = 23,
			.locbits = 12,
			.sigbits = 50},

		.lower = {}
	};

	if (0) {
		unsigned n = 1000, seed = 1;
		struct keymap sm{head, -1};
		struct shard *shard = new struct shard(&sm, sm.upper, -1, 18, 19);
		u64 sigmask = bitmask(sm.upper->sigbits);

		trace_on("%i shard inserts", n);
		for (unsigned j = 0; j < n; j++) {
			srand(seed);
			for (unsigned i = 0; i < n; i++)
				shard->insert(rand() & sigmask, i);
			if (0) {
				shard->empty();
				continue;
			}
			if (0)
				shard->dump(-1, "A ");
			srand(seed);
			for (unsigned i = 0; i < n; i++) {
				if (0)
					printf("%i:\n", i);
				shard->remove(rand() & sigmask, i);
				if (0 && !(i % 100))
					shard->dump(/*i >= 217 ? 0xf :*/7, "B ");
			}
		}
		if (0)
			shard->dump(-1, "B ");
		return 0;
	}

	if (argc <= 1)
		error_exit(1, "usage: %s <filename> <iterations>", argv[0]);

	int fd = open(argv[1], O_CREAT|O_RDWR, 0644);
	if (fd == -1)
		errno_exit(1);

	if (0) {
		struct keymap sm{head, -1};
		printf("blockbits %i tablebits %i stridebits %i\n", sm.blockbits, sm.tablebits, sm.upper->stridebits);
		sm.populate(0, 1);
		sm.map[0]->dump();
		return 0;
	}

	if (0) {
		unsigned n = 20, seed = 7;

		struct header head = {
			.magic = {'t', 'e', 's', 't'},
			.version = 0,
			.blockbits = 12,
			.tablebits = 4, // advisory
			.maxtablebits = 4,
			.reshard = 1, // power of 2
			.rehash = 2, // power of 2
			.loadfactor = one_fixed8,
			.upper = {
				.mapbits = 4,
				.stridebits = 22,
				.locbits = 12,
				.sigbits = 26,
			},
			.lower = {
				.mapbits = 2,
				.stridebits = 22,
				.locbits = 12,
				.sigbits = 28,
			},
		};

		struct keymap sm{head, fd};

		if (0) {
			struct shard *shard = new struct shard(&sm, sm.upper, 0, 3, 4);
			shard->insert(0x111111111, 0x998);
			shard->insert(0x222222222, 0x997);
			shard->insert(0x333333333, 0x998);
			shard->insert(0x444444444, 0x998);
			shard->dump(-1, "A ");
			shard->remove(0x222222222, 0x997);
			shard->remove(0x111111111, 0x998);
			shard->dump(-1, "B ");
			shard->insert(0x123456789, 0x998);
			shard->dump(-1, "C ");
			if (1) {
				struct shard *clone = new struct shard(&sm, sm.upper, 0, 3, 4);
				auto insert = [&clone](hashkey_t key, loc_t loc) { clone->insert(key, loc); };
				shard->walk(insert);
				clone->dump(-1, "X ");
			}
			return 0;
		}

		unsigned which = 4, factor = sm.tiershift(*sm.lower);
		struct shard *shard = new struct shard(&sm, sm.lower, which, sm.tablebits, sm.tablebits + 1);
		u64 sigmask = bitmask(sm.lower->sigbits);
		srand(seed);
		for (unsigned i = 0; i < n; i++) {
			u64 key = rand() & sigmask, loc = i;
			if (0)
				printf("key %lx loc %lx\n", key, loc);
			shard->insert(key, loc);
		}
		shard->dump(-1, "A ");
		sm.spam(shard);

		if (1) {
			sm.rehash(which, 1);
			sm.getshard(which)->dump(-1, "B ");
			sm.dump();
			return 0;
		}

		sm.dump();
		sm.reshard(which, factor, factor - 1);
		sm.dump();
		return 0;
	}

	return 0;
}

using namespace std;

//#undef trace
//#define trace trace_on
#include <sys/time.h>

int tpcb_run(int fds[5], unsigned scalefactor, unsigned iterations)
{
	/*
	 * Bench setup parameters
	 */
	enum {t_per_b = 10, a_per_b = 100000, seed = 2, filler = 88};

	/*
	 * Default table geometry (obscure, just works)
	 */
	struct header head = {
		.magic = {'t', 'e', 's', 't'},
		.version = 0,
		.blockbits = 14,
		.tablebits = 9,
		.maxtablebits = 19,
		.reshard = 1,
		.rehash = 2,
		.loadfactor = one_fixed8,
		.blocks = 0,

		.upper = {
			.mapbits = 0,
			.stridebits = 23,
			.locbits = 12,
			.sigbits = 50},

		.lower = {}
	};

	typedef u32 id;
	typedef s64 cash;
	struct account { id aid, bid; cash balance; u8 pad[84]; } __attribute__((packed));
	struct branch { id bid; cash balance; u8 pad[88]; } __attribute__((packed));
	struct teller { id tid, bid; cash balance; u8 pad[84]; } __attribute__((packed));
	struct transaction { id aid, tid, bid; cash balance; struct timeval tv; u8 pad[30 - sizeof(struct timeval)]; } __attribute__((packed));

	assert(sizeof(struct branch) == 100);
	assert(sizeof(struct account) == 100);
	assert(sizeof(struct teller) == 100);
	assert(sizeof(struct transaction) == 50);

	/*
	 * Transaction log in its own file
	 */
	struct pmblock *xlog;
	struct layout xlog_layout;
	xlog_layout.map.push_back({microlog_size, 12, (void **)&xlog, NULL});
	xlog_layout.do_maps(fds[0]);
	unsigned retail = 0; // redo log tail

	/*
	 * One kvs table per file
	 */
	struct keymap branches{head, fds[1], 100};
	struct keymap accounts{head, fds[2], 100};
	struct keymap tellers{head, fds[3], 100};
	struct keymap history{head, fds[4], 50};

	/*
	 * Provide these lists to driver to generate transactions
	 */
	vector<id> branch_id;
	vector<vector<id_t>> accounts_by_branch;
	vector<unsigned> teller_branch;
	vector<id> teller_id;

	/*
	 * Generate initial database prior to steady state bench
	 */
	id bid = 1, tid = 1, aid = 1;

	for (unsigned n = 0; n < scalefactor; n++, bid++) {
		struct branch data = { bid };
		memset(data.pad, filler, sizeof data.pad);
		branches.insert(&bid, 4, &data);
		branch_id.push_back(bid);

		for (int i = 0; i < t_per_b; i++) {
			struct teller data = { tid, bid };
			memset(data.pad, filler, sizeof data.pad);
			tellers.insert(&tid, 4, &data);
			teller_branch.push_back(n);
			teller_id.push_back(tid);
			tid++;
		}

		vector<id> accounts_at_branch;

		for (int i = 0; i < a_per_b; i++) {
			struct account data = { aid, bid };
			memset(data.pad, filler, sizeof data.pad);
			accounts.insert(&aid, 4, &data);
			accounts_at_branch.push_back(aid);
			aid++;
		}

		accounts_by_branch.push_back(accounts_at_branch);
	}

	/*
	 * The benchmark proper (driver and transactions)
	 */
	unsigned teller_count = teller_id.size();
	srand(seed);

	for (id hid = 1; hid <= iterations; hid++) {
		/* generate a random transaction. Note! 100% local transactions for now */
		unsigned i = rand() % teller_count, j = teller_branch[i];
		unsigned a = rand() % accounts_by_branch[j].size();
		id aid = accounts_by_branch[j][a];
		id bid = branch_id[j];
		id tid = teller_id[i];
		long delta_min = -999999, delta_max = +999999;
		long delta = (rand() % (unsigned)(delta_max - delta_min + 1)) + delta_min;

		/* Acquire transaction resources (one record from each of three tables) */
		rec_t *rec;
		struct query { struct account *a; struct branch *b; struct teller *t; } query = {};
		if ((rec = accounts.lookup(&aid, 4)))
			query.a = (struct account *)rec;
		if ((rec = branches.lookup(&bid, 4)))
			query.b = (struct branch *)rec;
		if ((rec = tellers.lookup(&tid, 4)))
			query.t = (struct teller *)rec;
		if ((!query.a|!query.b|!query.t))
			error_exit(1, "*** abort hid %u: aid %u bid %u tid %u (%i-%i-%i)",
				hid, aid, bid, tid, !!query.a, !!query.b, !!query.t);

		/* All resources were acquired so transaction is now guaranteed to succeed */
		/* Log redo record for replay in case of crash */
		struct redo { id hid, aid, tid, bid; cash delta, a, b, t; };
		struct redo redo = {hid, aid, tid, bid, delta, query.a->balance, query.b->balance, query.t->balance };
		log_commit(xlog, &redo, sizeof redo, &retail);

		/* update balances in memory mapped records */
		query.a->balance += delta;
		query.b->balance += delta;
		query.t->balance += delta;

		/* make update persistent */
		clwb(&query.a->balance);
		clwb(&query.b->balance);
		clwb(&query.t->balance);

		/* log transaction history (use an ordinary file in real life) */
		struct timeval tv;
		gettimeofday(&tv, NULL);
		struct transaction transaction = { aid, tid, bid, query.a->balance, tv };
		history.insert(&hid, 4, &transaction);
	}

	return 0;
}
