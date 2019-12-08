/*
 * Shardmap lightweight and fast key value store
 * (c) 2012 - 2019, Daniel Phillips
 * License: GPL v3
 */

#include <stdint.h>
#include <functional> // to pass lambdas to bucket walkers
#include <vector>

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint32_t u16;
typedef uint8_t u8;
typedef uint8_t rec_t;
typedef uint32_t loc_t;
typedef unsigned fixed8;
typedef int64_t s64;

/* cache line and pmem characteristics */

typedef uint64_t cell_t;
enum {logorder = 9, logsize = 1 << logorder, logmask = logsize - 1};
enum {cellshift = 3, cellsize = 1 << cellshift};
enum {lineshift = 6, linesize = 1 << lineshift, linemask = linesize - 1, linecells = linesize >> cellshift};
enum {blocklines = 4, blockcells = blocklines * linecells};
struct pmblock { cell_t data[blockcells]; };

/* some handy inlines (put these elsewhere!) */

static u64 power2(unsigned power) { return 1LL << power; }
static u64 power2(unsigned power, u64 value) { return value << power; }
static u64 bitmask(unsigned bits) { return power2(bits) - 1; }
static u64 align(u64 n, unsigned bits) { return n + (-n & bitmask(bits)); }

extern "C" { // bigmap.h
enum {bigmap_maxlevels = 10};

struct datamap
{
	uint8_t *data;
	loc_t loc;
};

struct bigmap
{
	unsigned blocksize, blockbits, levels;
	loc_t blocks, maxblocks;
	struct level {
		struct datamap map;
		uint16_t start, at, wrap, big;
	} path[bigmap_maxlevels];
	bool partial_path;
	uint8_t big;
	uint16_t reclen; // this is only here because some ext_bigmap functions need it. Fix!!!
	uint8_t *rbspace; // this is only here because we have not properly abstracted the block mapping yet!!!
};

/* Exports */

void bigmap_open(struct bigmap *map);
void bigmap_close(struct bigmap *map);
int bigmap_try(struct bigmap *map, unsigned len, unsigned big); // maybe bigmap should do ext_bigmap_big itself?
int bigmap_free(struct bigmap *map, loc_t loc, unsigned big);
size_t bigmap_check(struct bigmap *map);
void bigmap_dump(struct bigmap *map);
void bigmap_load(struct bigmap *map, loc_t loc); // rationalize me... inits path level[0]
void add_new_rec_block(struct bigmap *map);
bool is_maploc(loc_t loc, unsigned blockbits);

/* Imports */

uint8_t *ext_bigmap_mem(struct bigmap *map, loc_t loc);
void ext_bigmap_map(struct bigmap *map, unsigned level, loc_t loc);
void ext_bigmap_unmap(struct bigmap *map, struct datamap *dm);
unsigned ext_bigmap_big(struct bigmap *map, struct datamap *dm);
}

enum {one_fixed8 = 0x100};

typedef u32 count_t; // many places should use this instead of u32!!!
typedef u64 hashkey_t;

/* ...shardmap.h proper begins below */

/* Variable width field support */

struct duopack
{
	u64 mask;
	u8 bits0;
	duopack() = default; // so tier can be default-constructed in drop_tier
	duopack(const unsigned bits0) : mask(bitmask(bits0)), bits0(bits0) {}
	typedef cell_t T1;
	typedef loc_t T2;
} __attribute__((packed));

static u64 duo_pack(const struct duopack *duo, const duopack::T1 a, const duopack::T2 b) { return (power2(duo->bits0, b)) | a; }
static duopack::T1 duo_first(const struct duopack *duo, const u64 packed) { return packed & duo->mask; }
static duopack::T2 duo_second(const struct duopack *duo, const u64 packed) { return packed >> duo->bits0; }
static void duo_unpack(const struct duopack *duo, const u64 packed, duopack::T1 &a, duopack::T2 &b) { a = duo_first(duo, packed); b = duo_second(duo, packed); }

struct tripack
{
	u64 mask2;
	u8 bits0, bits1;
	tripack(const unsigned bits0, const unsigned bits1) : mask2(bitmask(bits0 + bits1)), bits0(bits0), bits1(bits1) {}
	typedef u32 T1;
	typedef u32 T2;
	typedef u64 T3;
};

static u64 tri_pack(const struct tripack *tri, const tripack::T1 a, const tripack::T2 b, const tripack::T3 c) { return power2(tri->bits0 + tri->bits1, c) | ((u64)b << tri->bits0) | a; }
static tripack::T1 tri_first(const struct tripack *tri, const u64 packed) { return packed & (tri->mask2 >> tri->bits1); }
static tripack::T2 tri_second(const struct tripack *tri, const u64 packed) { return (packed & tri->mask2) >> tri->bits0; }
static tripack::T3 tri_third(const struct tripack *tri, const u64 packed) { return packed >> (tri->bits0 + tri->bits1); }
static void tri_unpack(const struct tripack *tri, const u64 packed, tripack::T1 &a, tripack::T2 &b, tripack::T3 &c) { a = tri_first(tri, packed); b = tri_second(tri, packed); c = tri_third(tri, packed); }
static void tri_set_first(const struct tripack *tri, u64 &packed, const tripack::T1 value) { packed = (packed & ~(tri->mask2 >> tri->bits1)) | value; }

struct region { u64 /* is this right? */ size, align; void **mem; loff_t *pos; };

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
	duopack duo; // defines loc:sigbits variable width media image entries
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
} ;

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
	const tripack tri;
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

// recops.h inlined here...

/* record block format */

struct tabent { u8 hash; u8 len; }; /* record table entry */

struct rb
{
	u16 size; /* blocksize */
	u16 used; /* entry headers and key text stored compactly at top of block */
	u16 free; /* total key text space in free entries within entry region */
	u16 count; /* total entries including free entries */
	u16 holes; /* number of free entries created by delete */
	char magic[2];
	struct tabent table[];
};

enum {tabent_size = sizeof(struct tabent)};
enum {cleanup = 0, cleaned = 0xee, deleted = 0xdd, holecode = 0xff, maxname = 255};

static u8 rb_hash(u16 ihash) { return ihash % 255; }

struct recops {
	void (*init)(struct recinfo *ri);
	int (*big)(struct recinfo *ri);
	int (*more)(struct recinfo *ri);
	void (*dump)(struct recinfo *ri);
	void *(*key)(struct recinfo *ri, unsigned which, unsigned *ret);
	bool (*check)(struct recinfo *ri);
	rec_t *(*lookup)(struct recinfo *ri, const void *key, u8 len, u16 lowhash);
	rec_t *(*varlookup)(struct recinfo *ri, const void *key, u8 len, u16 lowhash, u8 *varlen);
	rec_t *(*create)(struct recinfo *ri, const void *newkey, u8 newlen, u16 lowhash, const void *newrec, u8 varlen);
	int (*remove)(struct recinfo *ri, const void *key, u8 len, u16 lowhash);
	int (*walk)(struct recinfo *ri, rb_walk_fn fn, void *context);
};

namespace fixsize {
	enum {taglen = 0}; // optional one byte variable data borrowed from key
	struct rb *rbi(struct recinfo *ri);
	struct rb *rbirec(struct recinfo *ri, rec_t **rec);
	unsigned rb_gap(struct recinfo *ri, struct rb *rb);
	void rb_init(struct recinfo *ri);
	int rb_big(struct recinfo *ri);
	int rb_more(struct recinfo *ri);
	void rb_dump(struct recinfo *ri);
	void *rb_key(struct recinfo *ri, unsigned which, unsigned *ret);
	bool rb_check(struct recinfo *ri);
	rec_t *rb_lookup(struct recinfo *ri, const void *key, u8 len, u16 lowhash);
	rec_t *rb_varlookup(struct recinfo *ri, const void *key, u8 len, u16 lowhash, u8 *varlen);
	rec_t *rb_create(struct recinfo *ri, const void *newkey, u8 newlen, u16 lowhash, const void *newrec, u8 varlen = 0);
	int rb_remove(struct recinfo *ri, const void *key, u8 len, u16 lowhash);
	int rb_walk(struct recinfo *ri, rb_walk_fn fn, void *context);
	extern struct recops recops;
}

// ...recops.h

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

	void *Private;
	struct pmblock *microlog, *upper_microlog;
	loff_t microlog_pos;
	loff_t rbspace_pos;

	unsigned loghead = 0, logtail = 0;

	struct layout layout;

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
