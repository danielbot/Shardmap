int tpcb_run(unsigned scalefactor, unsigned iterations)
{
	/*
	 * Official transaction generation ratios
	 */
	enum {t_per_b = 10, a_per_b = 100000, seed = 2, filler = 88};

	/*
	 * Default table geometry (obscure, just works)
	 */
	struct header head = {
		.magic = {'t', 'e', 's', 't'},
		.version = 0,
		.blockbits = 14,
		.bucketbits = 9,
		.maxbucketbits = 19,
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

	/*
	 * Each kvs table goes in a separate file
	 */
	int fds[4] = {
		open("table1", O_CREAT|O_RDWR, 0644),
		open("table2", O_CREAT|O_RDWR, 0644),
		open("table3", O_CREAT|O_RDWR, 0644),
		open("table4", O_CREAT|O_RDWR, 0644)};

	typedef u32 id;
	typedef u64 cash;
	struct account { id aid, bid; cash balance; u8 pad[84]; } __attribute__((packed));
	struct branch { id bid; cash balance; u8 pad[88]; } __attribute__((packed));
	struct teller { id tid, bid; cash balance; u8 pad[84]; } __attribute__((packed));
	struct transaction { id aid, tid, bid; cash balance; struct timeval tv; u8 pad[30 - sizeof(struct timeval)]; } __attribute__((packed));

	assert(sizeof(struct branch) == 100);
	assert(sizeof(struct account) == 100);
	assert(sizeof(struct teller) == 100);
	assert(sizeof(struct transaction) == 50);

	struct shardmap branches{head, fds[0], 100};
	struct shardmap accounts{head, fds[1], 100};
	struct shardmap tellers{head, fds[2], 100};
	struct shardmap history{head, fds[3], 50};

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
		branches.insert((u8 *)&bid, 4, 0, &data);
		branch_id.push_back(bid);

		for (int i = 0; i < t_per_b; i++) {
			struct teller data = { tid, bid };
			memset(data.pad, filler, sizeof data.pad);
			tellers.insert((u8 *)&tid, 4, 0, &data);
			teller_branch.push_back(n);
			teller_id.push_back(tid);
			tid++;
		}

		vector<id> accounts_at_branch;

		for (int i = 0; i < a_per_b; i++) {
			struct account data = { aid, bid };
			memset(data.pad, filler, sizeof data.pad);
			accounts.insert((u8 *)&aid, 4, 0x77, &data);
			accounts_at_branch.push_back(aid);
			aid++;
		}

		accounts_by_branch.push_back(accounts_at_branch);
	}

	/*
	 * The benchmark proper (driver and transactions)
	 */
	unsigned teller_count = teller_id.size();
	unsigned log_ring = 0; /* transaction redo log */
	srand(seed);

	for (id hid = 1; hid <= iterations; hid++) {
		/* generate a random transaction. Note! 100% local transactions for now */
		unsigned i = rand() % teller_count, j = teller_branch[i];
		unsigned a = rand() % accounts_by_branch[j].size();
		id aid = accounts_by_branch[j][a];
		id bid = branch_id[j];
		id tid = teller_id[i];

		/* Acquire transaction resources (one record from each of three tables) */
		struct rec *rec;
		struct query { struct account *a; struct branch *b; struct teller *t; } query = {};
		if ((rec = accounts.lookup((u8 *)&aid, 4)))
			query.a = (struct account *)rec->data;
		if ((rec = branches.lookup((u8 *)&bid, 4)))
			query.b = (struct branch *)rec->data;
		if ((rec = tellers.lookup((u8 *)&tid, 4)))
			query.t = (struct teller *)rec->data;
		if (1 || (!query.a|!query.b|!query.t))
			error_exit(1, "*** abort *** %i:[aid %u bid %u tid] %u %i %i %i",
				hid, aid, bid, tid, !!query.a, !!query.b, !!query.t);

		/* All resources were acquired so transaction is now guaranteed to succeed */
		long delta_min = -999999, delta_max = +999999;
		long delta = (rand() % (unsigned)(delta_max - delta_min + 1)) + delta_min;

		/* Log redo record for replay in case of crash */
		struct redo { id hid, aid, tid, bid; cash delta, a, b, t; };
		struct redo redo = {hid, aid, tid, bid, delta, query.a->balance, query.b->balance, query.t->balance };
		log_commit(history.macrolog, &redo, sizeof redo, &log_ring );

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
		history.insert((u8 *)&hid, 4, 0x99, &transaction );
	}

	return 0;
}
