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
