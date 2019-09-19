/*
 * Shardmap lightweight and fast key value store
 * (c) 2012 - 2019, Daniel Phillips
 * License: GPL v3
 */

/* Should these generic things really be here? ... no, only the standard headers */

#include <vector>
#include <functional> // to pass lambdas to bucket walkers

extern "C" {
#include "pmem.h" // for sidelog... don't expose this!
#include "bigmap.h" // C style base class for keymap
}

enum {one_fixed8 = 0x100};

typedef u32 count_t; // many places should use this instead of u32!!!
typedef u64 hashkey_t;

/* ...shardmap.h proper begins below */

/* Variable width field support */

template <class T1, class T2> struct duopack
{
	u64 mask;
	u8 bits0;

	duopack() = default;
	duopack(const unsigned bits0);
	u64 pack(const T1 a, const T2 b) const;
	void unpack(const u64 packed, T1 &a, T2 &b) const;
	T1 first(const u64 packed) const;
	T2 second(const u64 packed) const;
} __attribute__((packed));

template <class T1, class T2, class T3> struct tripack
{
	u64 mask2;
	u8 bits0, bits1;

	tripack(const unsigned bits0, const unsigned bits1);
	u64 pack(const T1 a, const T2 b, const T3 c) const;
	void unpack(const u64 packed, T1 &a, T2 &b, T3 &c) const;
	u64 first(const u64 packed) const;
	u64 second(const u64 packed) const;
	u64 third(const u64 packed) const;
	void set_first(u64 &packed, const T1 field) const;
};

struct region { u64 size, align; void **mem; loff_t *pos; };

struct layout
{
	enum { single_map = 1, verbose = 1 };

	std::vector<region> map;
	loff_t size = 0;
	void do_maps(int fd);
	void redo_maps(int fd);
};

struct header {
	char magic[4];
	u8 version;
	u8 blockbits;
	u8 tablebits, maxtablebits; // advisory buckets per shard
	u8 reshard, rehash; // power of 2
	u16 loadfactor; // fixed 8.8
	u32 blocks;
	struct tierhead {
		u8 mapbits, stridebits, locbits, sigbits;
		u32 maploc;
		bool is_empty() const { return !stridebits; }
	}  __attribute__((packed)) upper, lower;
} __attribute__((packed));

struct tier
{
	duopack <cell_t, loc_t> duo; // defines loc:sigbits variable width media image entries
	u8 mapbits, stridebits, locbits, sigbits, unused[3]; // sigbits not used in fast path, can derive from shardmap sigbits and difference between tier mapbits.
	count_t *countbuf; // front buffer
	count_t *countmap; // pmem, cannot be freed, please make it clear
	cell_t *shardmap;
	loff_t countmap_pos; // not really used!
	loff_t shardmap_pos; // not really used!

	tier(const struct header &header, const struct header::tierhead &tierhead);
	tier() = default;unsigned shards() const;
	bool is_empty() const;
	void cleanup();
	count_t *getbuf(unsigned size);
	cell_t *at(unsigned ix, unsigned i) const;
	void store(unsigned ix, unsigned i, cell_t entry) const;
} __attribute__((packed));

struct shard
{
	enum {endlist = 0, noentry = 1};
	unsigned used, free; // measured in hash links
	const unsigned top; // measured in hash links
	unsigned count; // measured in hash entries
	const unsigned limit; // measured in hash entries
	const u8 tablebits, lowbits;
	const u8 tx:1; // tier relative to current upper: 0 = upper, 1 = lower
	u16 ix:15; // shard index within tier map
	struct shard_entry { u64 key_loc_link; } *table;
	struct keymap *const map;
	const tripack <u32, u32, u64> trio;
	shard(struct keymap *map, const struct tier *tier, unsigned i, unsigned tablebits, unsigned linkbits);
	bool is_lower();
	const struct tier &tier() const;
	rec_t *lookup(const void *name, unsigned len, hashkey_t key);
	int insert(const hashkey_t key, const loc_t loc);
	int remove(const hashkey_t key, const loc_t loc);
	int remove(const void *name, unsigned len, hashkey_t key);
	unsigned buckets();
	bool bucket_used(const unsigned i);
	unsigned next_entry(const unsigned link);
	void set_link(unsigned prev, unsigned link);
	unsigned stride() const;
	void empty();
	count_t &mediacount() const;
	unsigned buckets() const;
	void imprint();
	void walk_bucket(std::function<void(hashkey_t key, loc_t loc)> fn, unsigned bucket);
	void walk_buckets(std::function<void(unsigned bucket)> fn);
	void walk(std::function<void(hashkey_t key, loc_t loc)> fn);
	void dump(const unsigned flags = -1, const char *tag = "") __attribute__((used));
	int load_from_media();
	int flatten();
	void reshard_part(struct shard *out, unsigned more_shards, unsigned part);
protected: // disallow stack instance because of self destruct
	~shard();
	friend class keymap; // allow delete
};

struct recinfo { const unsigned blocksize, reclen; u8 *data; loc_t loc; struct keymap *map;};

typedef void (rb_walk_fn)(void *context, u8 *key, unsigned keylen, u8 *data, unsigned reclen);

#include "recops.h"

#ifndef SIDELOG
#define SIDELOG
#endif

struct keymap : bigmap
{
	struct shard **map;
	struct tier tiers[2];
	struct tier *upper = tiers, *lower = tiers + 1;
	hashkey_t keymask;
	unsigned sigbits, mapmask, tablebits; // try u8 for a couple of these
	unsigned shards, pending;
	float loadfactor; // working as intended but obscure in places
	struct datamap peek; // for lookups
	struct header &header;
	const struct recops &recops;
	struct recinfo sinkbh;
	struct recinfo peekbh;
//	struct datamap header; // sm header including map geometry
	int fd;
	unsigned id;

	struct pmblock *microlog, *upper_microlog;
	loff_t microlog_pos;
	loff_t rbspace_pos;

	unsigned loghead = 0, logtail = 0;

	struct layout layout;

#ifdef SIDELOG
	struct sidelog
	{
		u64 duo;
		u32 at;
		u8 ix; // countmap index
		u8 rx; // relative tier index
		u8 flags;
		u8 unused;
	} sidelog[logsize];
#endif

	enum {reclen_default = 100};

	keymap(struct header &header, const int fd, struct recops &recops, unsigned reclen = reclen_default);

	struct shard *new_shard(const struct tier *tier, unsigned i, unsigned tablebits, bool virgin = 1);
	struct shard **mapalloc();
	~keymap();

	struct recinfo &sinkinfo();
	struct recinfo &peekinfo(loc_t loc);
	void spam(struct shard *shard);
	void spam(struct shard *shard_or_null, unsigned ix, unsigned shift);
	void dump(unsigned flags = 1);
	unsigned burst();
	const struct tier &tier(const struct shard *shard) const;
	unsigned tiershift(const struct tier &tier) const;
	bool single_tier() const;
	struct shard *populate(unsigned i, bool for_insert = 0);
	void populate_all();
	struct shard *getshard(unsigned i, bool for_insert = 1);
	struct shard *setshard(const unsigned i, struct shard *shard);
	static u64 shardmap_size(struct tier *tier);
	enum {map_rbspace = 0}; // rbspace layout map vector position for maxblocks
	void define_layout(std::vector<region> &map);
	int rehash(const unsigned i, const unsigned more);
	int reshard(const unsigned i, const unsigned more_shards, const unsigned more_buckets);
	void force_pending();
	int add_tier(const unsigned more);
	void drop_tier();
	int grow_map(const unsigned more);
	int reshard_and_grow(unsigned i);
	int insert_and_grow(struct shard *&shard, const hashkey_t key, const loc_t loc);
	void showlog();
	void checklog(unsigned flags);
	rec_t *insert(const void *name, unsigned namelen, const void *data, bool unique = 1);
	rec_t *insert(const char *name, unsigned namelen, const void *data, bool unique = 1);
	rec_t *lookup(const void *name, unsigned len);
	rec_t *lookup(const char *name, unsigned len);
	int remove(const void *name, unsigned len);
	int remove(const char *name, unsigned len);
    int unify();
};

/* misc cruft */

enum {microlog_size = logsize * sizeof (struct pmblock)};

/* some handy inlines */

static u64 power2(unsigned power) { return 1LL << power; }
static u64 power2(unsigned power, u64 value) { return value << power; }
static u64 bitmask(unsigned bits) { return power2(bits) - 1; }
static u64 align(u64 n, unsigned bits) { return n + (-n & bitmask(bits)); }
