/*
 * Shardmap fast lightweight key value store
 * (c) 2012 - 2019, Daniel Phillips
 * License: GPL v3
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>

#include "shardmap.h"

extern "C" {
#include "debug.h"
#include "utility.h"
#include "pmem.h"
}

#define warn trace_on
#define tracedump_on hexdump
#define tracedump trace_off
#define trace_geom trace_on
#define trace trace_off

#ifndef SIDELOG
#define SIDELOG
#endif

namespace fixsize {
	#include "recops.c"

	struct recops recops = {
		.init = rb_init,
		.big = rb_big,
		.more = rb_more,
		.dump = rb_dump,
		.key = rb_key,
		.check = rb_check,
		.lookup = rb_lookup,
		.varlookup = rb_varlookup,
		.create = rb_create,
		.remove = rb_remove,
		.walk = rb_walk,
	};
}

void errno_exit(unsigned exitcode);
void error_exit(unsigned exitcode, const char *reason, ...);
void hexdump(const void *data, unsigned size);

enum {PAGEBITS = 12, PAGE_SIZE = 1 << PAGEBITS};
enum {countshift = 2};
static const cell_t high64 = 1LL << 63;

static unsigned fls(unsigned n)
{
	unsigned bits = 32;
	if (!(n & 0xffff0000u)) { n <<= 16; bits -= 16; }
	if (!(n & 0xff000000u)) { n <<= 8; bits -= 8; }
	if (!(n & 0xf0000000u)) { n <<= 4; bits -= 4; }
	if (!(n & 0xc0000000u)) { n <<= 2; bits -= 2; }
	if (!(n & 0x80000000u)) { n <<= 1; bits -= 1; }
	if (!n) bits -= 1;
	return bits;
}

static unsigned log2_ceiling(unsigned n) { return fls(n - 1); }
static fixed8 mul8(fixed8 a, unsigned b) { return (a * b) >> 8; }

enum {split_order = 0, totalentries_order = 26};

#include <algorithm> // min
#include <utility> // swap
#include <string>

/* Variable width field packing */

typedef cell_t duo_t1;
typedef loc_t duo_t2;

static u64 duo_pack(const struct duopack *duo, const duo_t1 a, const duo_t2 b) { return (power2(duo->bits0, b)) | a; }
static duo_t1 duo_first(const struct duopack *duo, const u64 packed) { return packed & duo->mask; }
static duo_t2 duo_second(const struct duopack *duo, const u64 packed) { return packed >> duo->bits0; }
static void duo_unpack(const struct duopack *duo, const u64 packed, duo_t1 &a, duo_t2 &b) { a = duo_first(duo, packed); b = duo_second(duo, packed); }

typedef u32 tri_t1;
typedef u32 tri_t2;
typedef u64 tri_t3;

static u64 tri_pack(const struct tripack *tri, const tri_t1 a, const tri_t2 b, const tri_t3 c) { return power2(tri->bits0 + tri->bits1, c) | ((u64)b << tri->bits0) | a; }
static tri_t1 tri_first(const struct tripack *tri, const u64 packed) { return packed & (tri->mask2 >> tri->bits1); }
static tri_t2 tri_second(const struct tripack *tri, const u64 packed) { return (packed & tri->mask2) >> tri->bits0; }
static tri_t3 tri_third(const struct tripack *tri, const u64 packed) { return packed >> (tri->bits0 + tri->bits1); }
static void tri_unpack(const struct tripack *tri, const u64 packed, tri_t1 &a, tri_t2 &b, tri_t3 &c) { a = tri_first(tri, packed); b = tri_second(tri, packed); c = tri_third(tri, packed); }
static void tri_set_first(const struct tripack *tri, u64 &packed, const tri_t1 value) { packed = (packed & ~(tri->mask2 >> tri->bits1)) | value; }

/* Persistent memory */

#ifdef SIDELOG
struct sidelog
{
	u64 duo;
	u32 at;
	u8 ix; // countmap index
	u8 rx; // relative tier index
	u8 flags;
	u8 unused;
};

enum {sidelog_size = logsize * sizeof(struct sidelog)};
#endif

/* Memory layout setup */

void layout::do_maps(int fd)
{
	unsigned count = map.size();
	if (verbose)
		printf("%i regions:\n", count);
	loff_t pos = 0;
	for (unsigned i = 0; i < count; pos += map[i++].size) {
		if (!map[i].size)
			continue;
		pos = align(pos, map[i].align);
		if (verbose)
			printf("%i: %lx/%lx\n", i, pos, map[i].size);
		if (!map[i].pos)
			continue;
		*map[i].pos = pos;
	}
	size = align(pos, PAGEBITS);
	if (verbose)
		printf("*: %lx\n", size);

	void *base;
	if (single_map)
		base = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

	pos = 0;
	for (unsigned i = 0; i < count; pos += map[i++].size) {
		if (map[i].size) {
			if (!map[i].mem)
				continue;
			if (single_map) {
				*map[i].mem = (char *)base + pos;
				continue;
			}
			void *mem = mmap(NULL,
				align(map[i].size, PAGEBITS),
				PROT_READ|PROT_WRITE,
				MAP_SHARED, fd, pos);
			if (mem == MAP_FAILED)
				errno_exit(1);
			*map[i].mem = mem;
		}
	}

	if (ftruncate(fd, size))
		errno_exit(1);
}

void layout::redo_maps(int fd)
{
	assert(single_map);
	if (munmap(*map[0].mem, size) == -1)
		errno_exit(1);
	do_maps(fd);
}

tier::tier(const struct header &header, const struct header::tierhead &tierhead) :
	duo(tierhead.sigbits),
	mapbits(tierhead.mapbits),
	stridebits(tierhead.stridebits),
	locbits(tierhead.locbits),
	sigbits(tierhead.sigbits),
	countbuf(tierhead.is_empty() ? NULL : getbuf(power2(mapbits + countshift))),
	countmap(NULL),
	shardmap(NULL),
	countmap_pos(0), shardmap_pos(0)
	{}

#if 0
tier::~tier()
{
	trace_off("destroy %p countbuf %p", this, countbuf);
}
#endif

unsigned tier::shards() const { return power2(mapbits); }
bool tier::is_empty() const { return !stridebits; }

void tier::cleanup()
{
	trace("free countbuf %p", countbuf);
	free(countbuf);
	countbuf = NULL;
}

count_t *tier::getbuf(unsigned size)
{
	count_t *buf = (count_t *)aligned_alloc(size, size); // check null!!!
	trace("tier %p countbuf %p", this, buf);
	memset(buf, 0, size);
	return buf;
}

cell_t *tier::at(unsigned ix, unsigned i) const
{
	assert(ix < 1U << mapbits);
	assert(i < 1U << (stridebits - cellshift));
	return shardmap + power2(stridebits - cellshift, ix) + i;
}

void tier::store(unsigned ix, unsigned i, cell_t entry) const
{
	if (1)
		ntstore64(at(ix, i), entry);
	else
		*at(ix, i) = entry;
}

struct fifo
{
	cell_t *base, *tail, *top;

	fifo(cell_t *base, unsigned size) : base(base), tail(base), top(base + size) {}

	bool full() { return tail == top; }
	unsigned size() { return tail - base; }

	void push(cell_t entry)
	{
		assert(!full());
		*tail++ = entry;
	}
};

struct media_fifo : fifo // what is this??
{
	media_fifo(const struct tier &tier, const unsigned i) :
		fifo(tier.at(i, 0), power2(tier.stridebits - cellshift))
		{}
};

unsigned shard::buckets() { return power2(tablebits); }
bool shard::bucket_used(const unsigned i) { return table[i].key_loc_link != noentry; }
unsigned shard::next_entry(const unsigned link) { return tri_first(&tri, table[link].key_loc_link); }
void shard::set_link(unsigned prev, unsigned link) { tri_set_first(&tri, table[prev].key_loc_link, link); }
unsigned shard::stride() const { return power2(tier().stridebits); } // hardly used!

void shard::empty()
{
	unsigned n = used = buckets();
	for (unsigned i = 0; i < n; i++)
		table[i].key_loc_link = noentry;
	free = count = 0;
}

#if 0
void shard::rewind()
{
	fifo.tail = fifo.base + 1;
}
#endif
count_t &shard::mediacount() const { return tier().countbuf[ix]; }
unsigned shard::buckets() const { return power2(tablebits); }

void shard::imprint()
{
	struct {char a[4]; u16 b[2];} magic = {
		{'f', 'i', 'f', 'o'},
		{ix, tier().shards()}};
	memcpy(tier().at(ix, 0), &magic, 8);
	mediacount() = 1;
}

#if 0
void shard::append(cell_t value) // hardly used!
{
	count_t &count = mediacount();
	assert(count < top); // wrong!!!
	if (1) {
		assert(tier().at(ix, count) == fifo.tail);
		fifo.push(value);
	} else
		tier().store(ix, count, value);
	count++;
}
#endif

void shard::walk_bucket(std::function<void(hashkey_t key, loc_t loc)> fn, unsigned bucket)
{
	for (unsigned link = bucket;;) {
		u64 lowkey;
		u32 next, loc;
		tri_unpack(&tri, table[link].key_loc_link, next, loc, lowkey);
		fn(power2(lowbits, bucket) | lowkey, loc);
		if (next == endlist)
			break;
		link = next;
	}
}

void shard::walk_buckets(std::function<void(unsigned bucket)> fn)
{
	for (unsigned bucket = 0; bucket < buckets(); bucket++)
		if (bucket_used(bucket))
			fn(bucket);
}

void shard::walk(std::function<void(hashkey_t key, loc_t loc)> fn)
{
	for (unsigned bucket = 0; bucket < buckets(); bucket++)
		if (bucket_used(bucket))
			walk_bucket(fn, bucket);
}

void shard::dump(const unsigned flags, const char *tag)
{
	printf("%sshard %i:%i ", tag, is_lower(), ix);
	unsigned tablesize = buckets(), found = 0, empty = tablesize;

	if (flags & 1)
		printf("buckets %u entries %u used %u top %u lowbits %u\n",
			tablesize, count, used, top, lowbits);

	if (flags & (2|4)) {
		auto f2 = [flags, &found](hashkey_t key, loc_t loc) {
			if (flags & 2)
				printf("%lx:%x ", key, loc);
			found++;
		};

		auto f1 = [this, f2, flags, tag, tablesize, &empty](unsigned bucket) {
			if (flags & 2)
				printf("%s(%x/%x) ", tag, bucket, tablesize);
			walk_bucket(f2, bucket);
			empty--;
		};

		walk_buckets(f1);
		if (flags & 2)
			printf("\n");
	}

	if (flags & 4)
		printf("(%u entries, %u buckets empty)\n", found, empty);

	if (flags & 10) {
		if (free) {
			printf("free:");
			for (unsigned link = free; link; link = next_entry(link))
				printf(" %u", link);
			printf("\n");
		}
	}
	if (flags & (2|4))
		assert(found == count);
}

int shard::load_from_media()
{
	enum {verbose = 0};

	unsigned n = mediacount();
	trace("%i entries", n);
	cell_t *media = tier().at(ix, 0);

	for (unsigned j = 1; j < n; j++) {
		cell_t entry = media[j];
		bool is_insert = !(entry & high64);
		entry &= ~high64;
		u64 key;
		loc_t loc;
		duo_unpack(&tier().duo, entry, key, loc);

		if (verbose)
			printf("%c%lx:%x%c", "-+"[is_insert], key, loc, " \n"[!(j % 10)]);

		if (!entry && j + 1 < n && !media[j + 1]) {
			/*
			 * Zero is actually a valid entry (key 0, block 0, delete)
			 * so paranoia check for two successive zeroes. Possibly a
			 * legal collision, but nearly certain to be corruption.
			 */
			warn("\nNull entry %i/%i", j, n);
			hexdump(media + j, 64);
			BREAK;
		}

		if (is_insert)
			insert(key, loc);
		else
			remove(key, loc);
	}
	if (verbose)
		printf("\n");

	if (0)
		fprintf(stderr, ".");

	return 0;
}

/*
 * Rewrite a media shard from cache to squeeze out tombstones
 */
int shard::flatten()
{
	trace("shard %u buckets %u entries %u", ix, buckets(), count);
	struct media_fifo media(tier(), ix);
	for (unsigned bucket = 0; bucket < buckets(); bucket++) {
		if (bucket_used(bucket)) {
			unsigned link = bucket;
			while (1) {
				u64 lowkey;
				u32 next, loc;
				tri_unpack(&tri, table[link].key_loc_link, next, loc, lowkey);
				hashkey_t key = power2(lowbits, bucket) | lowkey;
				trace_off("[%x] %lx => %lx", bucket, key, loc);
				media.push(duo_pack(&tier().duo, key, loc));
				if (!next)
					break;
				link = next;
			}
		}
	}
	mediacount() = media.size();
	assert(mediacount() <= count + 1);
	if (0)
		hexdump(tier().at(ix, 0), power2(cellshift, mediacount()));
	return 0;
}

void shard::reshard_part(struct shard *out, unsigned more_shards, unsigned part)
{
	unsigned partbits = tablebits - more_shards;
	trace("reshard x%li buckets %u entries %u", power2(more_shards), out->buckets(), count);
	for (unsigned bucket = part << partbits; bucket < (part + 1) << partbits; bucket++) {
		assert(bucket < buckets());
		if (bucket_used(bucket)) {
			unsigned link = bucket;
			while (1) {
				u64 lowkey;
				u32 next, loc;
				tri_unpack(&tri, table[link].key_loc_link, next, loc, lowkey);
				hashkey_t key = power2(lowbits, bucket) | lowkey;
				trace_off("[%x] %lx => %lx", bucket, key, loc);
				out->insert(key, loc);
				if (!next)
					break;
				link = next;
			}
		}
	}
}

shard::~shard() { ::free(table); }

// is there a better place to put these???
unsigned guess_linkbits(const unsigned tablebits, const fixed8 loadfactor)
{
	assert(loadfactor >= one_fixed8); // load factor less than one not supported yet
	//log2_ceiling(mul8(map->loadfactor, 2 << map->head.maxtablebits)) + cache_entry_bits;
	unsigned linkbits = tablebits + log2_ceiling(loadfactor) - 8 + 1; // fake mul8, obscure!
	trace_off("linkbits %u", linkbits);
	return linkbits;
}

loc_t choose_maploc(const unsigned mapbits, const unsigned stridebits, const unsigned blocksize, const unsigned blocks)
{
	enum {nominal_large_entry_size = 108}; // needs work!
	u64 max_entries_below = power2(mapbits + stridebits - cellshift);
	loc_t likely_entries_per_block = blocksize / nominal_large_entry_size;
	loc_t likely_blocks = max_entries_below / likely_entries_per_block;
	return std::max(likely_blocks, 2 * blocks);
}

unsigned calc_sigbits(const unsigned tablebits, const fixed8 loadfactor, const unsigned locbits)
{
	unsigned linkbits = guess_linkbits(tablebits, loadfactor);
	unsigned sigbits = 64 - linkbits + tablebits - locbits;
	trace_geom("tablebits %u locbits %u linkbits %u = %u", tablebits, locbits, linkbits, sigbits);
	return sigbits;
}

static unsigned mapid = 1; // could be different every run, is that ok??

keymap::keymap(struct header &header, const int fd, struct recops &recops, unsigned reclen) :
	bigmap(), // unfortunately impossible to initialize bigmap members here
	map(0), tiers({{header, header.upper}, {header, header.lower}}),
	tablebits(header.tablebits),
	shards(power2(upper->mapbits)), pending(0),
	loadfactor(header.loadfactor),
	peek({NULL, -1}),
	header(header),
	recops(recops),
	sinkbh{power2(header.blockbits), reclen, 0, 0, this},
	peekbh{power2(header.blockbits), reclen, 0, 0, this},
	fd(fd), id(mapid++), Private(0)
{
	printf("upper mapbits %u stridebits %u locbits %u sigbits %u\n",
		upper->mapbits, upper->stridebits, upper->locbits, upper->sigbits);
	map = mapalloc();
	blockbits = header.blockbits; // cannot initialize aggregate in
	blocksize = power2(blockbits); // initializer list only because
	bigmap::reclen = reclen; // the standard is lame.
	keymask = bitmask(upper->mapbits + (sigbits = upper->sigbits)); // need redundant sigbits field???
	mapmask = bitmask(upper->mapbits); // need redundant mapmask field???

	if (fd > 0) {
		define_layout(layout.map);
		layout.do_maps(fd);
		bigmap_open(this);
		u8 *frontbuf = (u8 *)aligned_alloc(power2(cellshift, blockcells), blocksize);
		path[0].map = (struct datamap){.data = frontbuf};
		maxblocks = layout.map[map_rbspace].size >> blockbits;
		add_new_rec_block(this);
		recops.init(&sinkinfo());
		log_clear(microlog);
#ifdef SIDELOG
		Private = (struct sidelog *)calloc(1, sidelog_size);
#endif
	}

	if (0)
		populate_all();
}

struct shard *keymap::new_shard(const struct tier *tier, unsigned i, unsigned tablebits, bool virgin)
{
	struct shard *shard = new struct shard(this, tier, i, tablebits, guess_linkbits(tablebits, loadfactor));
	if (virgin)
		shard->imprint();
	return shard;
}

struct shard **keymap::mapalloc()
{
	unsigned bytes = shards * sizeof *map;
	struct shard **map = (struct shard **)malloc(bytes);
	memset(map, 0, bytes);
	return map;
}

keymap::~keymap()
{
	for (unsigned i = 0; i < levels; i++)
		free(path[i].map.data); // danger!!! We assume these are front buffers

	for (unsigned i = 0; i < shards; i++) {
		struct shard *shard = map[i];
		if (shard) {
			spam(NULL, shard->ix, tiershift(tier(shard)));
			delete shard; // only delete lower tier shard once
		}
	}

	tiers[0].cleanup();
	tiers[1].cleanup();
	free(map);
#ifdef SIDELOG
	free(Private);
#endif
}

struct recinfo &keymap::sinkinfo()
{
	sinkbh.data = path[0].map.data; // would like to get rid of this assignment
	return sinkbh; // maybe by using struct ri directly in path[] and losing datamap
}

struct recinfo &keymap::peekinfo(loc_t loc)
{
	if (loc == path[0].map.loc)
		return sinkinfo();
	peek = (struct datamap){.data = peekbh.data = ext_bigmap_mem(this, loc), .loc = loc};
	return peekbh;
}

void keymap::spam(struct shard *shard)
{
	spam(shard, shard->ix, tiershift(tier(shard)));
}

void keymap::spam(struct shard *shard_or_null, unsigned ix, unsigned shift)
{
	for (unsigned i = ix << shift, n = power2(shift), j = i + n; i < j; i++)
		map[i] = shard_or_null;
}

void keymap::dump(unsigned flags)
{
	if (1) {
		printf("map: ");
		for (unsigned i = 0; i < shards; i++)
			printf("%c", "+*-"[!map[i] ? 2 : map[i]->is_lower()]);
		printf("\n");
		if (0)
			return;
	}
	for (unsigned i = 0; i < shards; i++) {
		struct shard *shard = map[i];
		if (!shard)
			continue;
		unsigned tiermask = bitmask(tiershift(tier(shard)));
		if (shard->is_lower()) {
			unsigned j = i & tiermask;
			if (j) {
				if (map[i & ~tiermask] == shard)
					continue;
				printf("*** wrong *** ");
			}
		}
		map[i]->dump(flags, "");
	}
}

unsigned keymap::burst() { return (logtail - loghead) & logmask; }
const struct tier &keymap::tier(const struct shard *shard) const { return tiers[shard->tx]; }
unsigned keymap::tiershift(const struct tier &tier) const { return upper->mapbits - tier.mapbits; }
bool keymap::single_tier() const { return !pending; }

struct shard *keymap::populate(unsigned i, bool for_insert)
{
	assert(!map[i]);
	struct tier *tier = single_tier() || upper->countbuf[i] ? upper : lower;
	unsigned count = tier->countbuf[i >> tiershift(*tier)];
	if (!count && !for_insert)
		return NULL;
	struct shard *shard = new_shard(tier, i, tablebits, !count);
	trace("shard %i:%i/%i", shard->is_lower(), i, count);
	shard->load_from_media();
	spam(shard);
	return shard;
}

void keymap::populate_all()
{
	for (unsigned i = 0; i < shards; i++)
		populate(i, 1);
}

struct shard *keymap::getshard(unsigned i, bool for_insert)
{
	assert(i < shards);
	return map[i] ? map[i] : populate(i, for_insert);
}

struct shard *keymap::setshard(const unsigned i, struct shard *shard)
{
	assert(shard->ix = i >> tiershift(tier(shard)));
	assert(!map[i]);
	trace("map[%i] = %p", i, shard);
	return map[i] = shard;
}

u64 keymap::shardmap_size(struct tier *tier)
{
	return power2(tier->stridebits + tier->mapbits) + power2(tier->stridebits);
}

void keymap::define_layout(std::vector<region> &map)
{
	u64 rbspace_size = power2(12 + 20);
	u64 upper_countmap_size = power2(upper->mapbits + countshift);
	u64 upper_shardmap_size = shardmap_size(upper);
	void **microlog_mem = (void **)&microlog;
	upper_microlog = NULL;

	map.push_back({rbspace_size, 12, (void **)&rbspace, &rbspace_pos});
	if (!lower->is_empty()) {
		u64 lower_countmap_size = power2(lower->mapbits + countshift);
		u64 lower_shardmap_size = shardmap_size(lower);
		map.push_back({microlog_size, 12, microlog_mem, NULL});
		map.push_back({lower_countmap_size, 12, (void **)&lower->countmap, &lower->countmap_pos});
		map.push_back({lower_shardmap_size, 12, (void **)&lower->shardmap, &lower->shardmap_pos});
		microlog_mem = NULL;
	}
	map.push_back({microlog_size, 12, microlog_mem ? : (void **)&upper_microlog, NULL});
	map.push_back({upper_countmap_size, 12, (void **)&upper->countmap, &upper->countmap_pos});
	map.push_back({upper_shardmap_size, 12, (void **)&upper->shardmap, &upper->shardmap_pos});
}

int keymap::rehash(const unsigned i, const unsigned more)
{
	trace_geom("[%u] shard %u buckets 2^%u -> 2^%u", id, i, tablebits, tablebits + more);
	unsigned out_tablebits = header.tablebits = tablebits = tablebits + more; // quietly adjusts default tablebits!
	struct shard *shard = map[i];
	struct shard *newshard = new_shard(&tier(shard), i, out_tablebits, 0);
	trace("entries %u locbits %u linkbits %u", shard->count, tiers[newshard->tx].locbits, newshard->trio.bits0);
	shard->reshard_part(newshard, 0, 0);
	assert(newshard->count == shard->count);
	spam(newshard); // support lower tier rehash even though it should never happen
	delete shard;
	return 0;
}

int keymap::reshard(const unsigned i, const unsigned more_shards, const unsigned more_buckets)
{
	struct shard *shard = map[i];
	assert(shard->is_lower());
	unsigned more_per_shard = more_buckets - more_shards;
	unsigned out_tablebits = tablebits + more_per_shard;
	trace("reshard %u 2^%i (2^%i buckets per shard)", i, more_shards, out_tablebits);
	for (unsigned part = 0, parts = power2(more_shards); part < parts; part++) {
		unsigned j = i + part;
		struct shard *newshard = new_shard(upper, j, out_tablebits);
		trace("map[%i] part %i", j, part);
		shard->reshard_part(newshard, more_shards, part);
		assert(map[j] == shard);
		map[j] = newshard;
		newshard->flatten();
	}
	shard->mediacount() = 0;
//	shard_unmap(shard);
	delete shard;
	return 0;
}

void keymap::force_pending() // untested
{
	if (pending) {
		trace_on("force %u pending", pending);
		unsigned more = upper->mapbits - lower->mapbits;
		for (unsigned i = 0; i < shards; i += power2(more)) {
			struct shard *shard = getshard(i, 0);
			if (shard && shard->is_lower()) {
				reshard(i, more, more); // error return!
				pending--;
			}
		}
		assert(!pending);
		drop_tier();
	}
}

/*
 * Significant bits is the sum of tablebits and explicitly stored hash
 * bits. We want as many significant bits as possible to maximize the
 * chance that key existence can be determined from the hash index alone.
 * Each new level, significant bits are expected to decrease because the
 * block location field gets bigger, roughly tracking the increase in
 * shards, but this is only approximate because it depends on the average
 * record size, which can change systematically over time.
 *
 * So we expect that significant bits will always decrease, roughly at
 * the same rate that shards increase. Because some hash bits are
 * implied by the shard index, hash precision stays nearly the same,
 * but is never allowed to increase because existing index entries would
 * then become inaccessible due to not storing the additional hash bits.
 *
 * We use the current blocks count to estimate the number of blocks
 * the next index level will have, a multiple of the current level. If
 * the estimate is too high, hash precision will be lost. However, this
 * loss of precision will not accumulate per level because the next
 * estimate will be based on the actual blocks used. If the estimate is
 * too low then the blocks field will fill up, and we will be forced to
 * add a new tier on short notice, skipping the nicety of incremental
 * reshard. Likewise, the block storage region may run into the bottom
 * of the index, which we try to place relatively low in the file for
 * esthetic and efficiency reasons.
 */
int keymap::add_tier(const unsigned more)
{
	unsigned locbits = log2_ceiling(blocks << (more + 1));
	if (locbits < upper->locbits) {
		/*
		 * Block address bits must never decrease because hash precision
		 * would increase, so lookup would fail for existing entries.
		 */
		warn("*** force non decreasing block address bits");
		locbits = upper->locbits;
	}

	unsigned sigbits = calc_sigbits(tablebits, loadfactor, locbits);
	unsigned maxsig = upper->sigbits - more;
	if (sigbits > maxsig)
		sigbits = maxsig;

	unsigned mapbits = upper->mapbits + more, stridebits = upper->stridebits;
	loc_t maploc = choose_maploc(mapbits, stridebits, blocksize, blocks);

	assert(lower->is_empty()); // Will swap then overwrite empty tier
	std::swap(lower, upper); // Existing references from shard remain valid
	header.lower = header.upper; // Update persistent part by overwrite
	header.upper = { // Define new upper tier
		.mapbits = mapbits,
		.stridebits = stridebits,
		.locbits = locbits,
		.sigbits = sigbits,
		.maploc = maploc };

	*upper = {header, header.upper}; // overwrite empty former lower tier

	layout.map.clear();
	define_layout(layout.map);
	layout.redo_maps(fd);

	trace_geom("mapbits %u maploc %x mapsize %lx filesize %lx sigbits %u locbits %u",
		mapbits, maploc, layout.size - upper->countmap_pos, layout.size,
		upper->sigbits, upper->locbits);

	return 0;
}

void keymap::drop_tier()
{
	trace_geom("drop tier");
	assert(!pending);
	unify(); // retire any lower tier updates still in flight

	unsigned any = 0;
	for (unsigned i = 0; i < lower->shards(); i++)
		any += lower->countbuf[i];
	assert(!any);

	free(lower->countbuf);
	*lower = (struct tier){};
	header.lower = (struct header::tierhead){};
	assert(loghead == logtail);
	microlog = upper_microlog;
}

int keymap::grow_map(const unsigned more)
{
	trace_geom("expand map x%u to 2^%u", 1 << more, upper->mapbits + more);
	assert(!pending);

	pending = shards;
	shards <<= more;
	sigbits -= more; // because shard index implies additional hash bits

	struct shard **oldmap = map, **newmap = mapalloc();
	for (unsigned i = 0; i < shards; i++) // unnecessary to zero in alloc
		newmap[i] = oldmap[i >> more];

	free(oldmap);
	map = newmap;
	return add_tier(more);
}

int keymap::reshard_and_grow(unsigned i)
{
	unsigned more_shards = header.reshard;
	struct shard *shard = map[i];
	int err;

	if (!single_tier() && !shard->is_lower()) {
		/*
		 * A nonuniform hash distribution could cause an upper tier shard to become
		 * much larger than lower tier shards not yet split, so the upper tier shard
		 * must be split, but first split all remaining lower tier shards.
		 */
		warn("*** unbalanced shard size forced shard splits");
		force_pending();
	}

	if (pending) {
		assert(upper->mapbits);
		more_shards = upper->mapbits - lower->mapbits;
		i &= ~bitmask(more_shards);
	} else {
		tablebits = header.tablebits = header.maxtablebits;
		grow_map(more_shards);
		i <<= more_shards;
	}

	int more_buckets = more_shards + header.maxtablebits - tablebits;
	if (more_buckets < 0) {
		/*
		 * At the transition from (ing to resharding some combination of geometry
		 * parameters and load could result in trying to reduce number of buckets at
		 * reshard, which is not supported.
		 */
		warn("*** more_buckets was negative");
		more_buckets = 0;
	}

	trace("reshard %i:%u pending %u", shard->is_lower(), i, pending);
	if ((err = reshard(i, more_shards, more_buckets)))
		return err;

	assert(pending);
	if (!--pending)
		drop_tier();

	return 0;
}

int keymap::insert_and_grow(struct shard *&shard, const hashkey_t key, const loc_t loc)
{
	assert(!(key & ~keymask));
	trace_off("%i:%u(%u) <= %lx:%x", shard->tx, shard->ix, shard->mediacount(), key, loc);
	if (shard->count == shard->limit) {
		trace("media %u:%u fullness %u/%u", shard->is_lower(), shard->ix, shard->mediacount(), shard->limit);
		trace("shards %u tablebits %u header.maxtablebits %u", shards, tablebits, header.maxtablebits);
		bool should_rehash = shards == 1 && header.maxtablebits > tablebits;
		unsigned more = std::min((unsigned)header.rehash, header.maxtablebits - tablebits);
		unsigned i = key >> sigbits;
		int err = should_rehash ? rehash(i, more) : reshard_and_grow(i);
		if (err)
			return err;
		shard = map[key >> sigbits]; // shard always changes; reshard changes sigbits
	}
	assert(shard->count < shard->limit);
	return shard->insert(key, loc);
}

rec_t *keymap::lookup(const char *key, unsigned len)
{
	return lookup((const u8 *)key, len);
}

rec_t *keymap::insert(const char *key, unsigned len, const void *data, bool unique)
{
	return insert((const u8 *)key, len, data, unique);
}

int keymap::remove(const char *key, unsigned len)
{
	return remove((const u8 *)key, len);
}

rec_t *keymap::lookup(const void *key, unsigned len)
{
	hashkey_t hash = keyhash(key, len) & keymask;
	struct shard *shard = getshard(hash >> sigbits, 0);
	return shard ? shard->lookup(key, len, hash) : NULL;
}

shard::shard(struct keymap *map, const struct tier *tier, unsigned i, unsigned tablebits, unsigned linkbits) :
	free(endlist), top(power2(linkbits)), // should depend on limit!!!
	count(0), limit(mul8(map->loadfactor, power2(tablebits))), // must not be more than cells(stride) - 1 (magic)
	tablebits(tablebits), lowbits(tier->sigbits - tablebits), tx(tier - map->tiers), ix(i >> map->tiershift(*tier)),
	map(map), tri(linkbits, tier->locbits)
{
	assert(tablebits <= linkbits);
	assert(power2(cellshift, top) <= power2(tier->stridebits));
	table = (struct shard_entry *)malloc(top * sizeof *table);
	assert(table); // do something!
	trace("buckets %i limit %i top %i lowbits %u linkbits %u", (int)power2(tablebits), limit, top, lowbits, tablebits);
	empty();
}

const struct tier &shard::tier() const { return map->tiers[tx]; }
bool shard::is_lower() { return tx == map->lower - map->tiers; }

unsigned long tests = 0, probes = 0;

rec_t *shard::lookup(const void *key, unsigned len, hashkey_t hash)
{
	trace("find '%s'", cprinz(key, len));
	cell_t lowhash = hash & bitmask(lowbits);
	unsigned link = (hash >> lowbits) & bitmask(tablebits);
	trace("hash %lx ix %i:%x bucket %x", hash, is_lower(), ix, link);
	if (bucket_used(link)) {
		tests++;
		do {
			const cell_t &entry = table[link].key_loc_link;
			if (tri_third(&tri, entry) == lowhash) {
				loc_t loc = tri_second(&tri, entry);
				trace("probe block %i:%x", map->id, loc);
				probes++;
				struct recinfo &ri = map->peekinfo(loc);
				rec_t *rec = map->recops.lookup((struct recinfo *)&ri, key, len, hash);
				if (rec)
					return rec;
			}
			link = next_entry(link);
		} while (link != endlist);
	}
	return NULL;
}

int shard::insert(const hashkey_t key, const loc_t loc)
{
	const unsigned bucket = (key >> lowbits) & bitmask(tablebits);
	trace("insert key %lx ix %i:%i:%x bucket %x loc %x", key, map->id, is_lower(), ix, bucket, loc);
	assert(bucket < buckets());
	assert(!((loc + 1) & ~bitmask(tier().locbits))); // overflow paranoia
	unsigned next = endlist;

	if (bucket_used(bucket)) {
		if (free != endlist) {
			trace("reuse 0x%x", free);
			next = free;
			free = next_entry(free);
		} else {
			if (used == top) {
				trace_on("out of room at %i", count);
				assert(0);
				return 1;
			}
			next = used++;
		}
		table[next] = table[bucket];
	}
	trace_off("set_entry key 0x%Lx => 0x%x", (long long)key, loc);
	table[bucket] = {tri_pack(&tri, next, loc, key & bitmask(lowbits))};
	count++;
	return 0;
}

int shard::remove(const hashkey_t key, const loc_t loc)
{
	const unsigned bucket = (key >> lowbits) & bitmask(tablebits);

	if (!bucket_used(bucket))
		return -ENOENT;

	const unsigned shift0 = tri.bits0, shift2 = tri.bits1 + tri.bits0;
	const u64 pairdata = power2(shift2, key & bitmask(lowbits)) | power2(shift0, loc);
	const u64 pairmask = -1ULL << shift0, linkmask = ~pairmask;
	u64 entry = table[bucket].key_loc_link;
	unsigned next = entry & linkmask;

	trace("delete key %lx bucket %x loc %x", key, bucket, loc);

	if ((entry & pairmask) == pairdata) {
		count--;
		if (next == endlist) {
			trace("clear bucket %x", next);
			table[bucket] = {noentry};
			return 0;
		}
		trace("pop bucket");
		table[bucket] = table[next];
		tri_set_first(&tri, table[next].key_loc_link, free);
		free = next;
		return 0;
	}

	for (unsigned link = bucket; next != endlist;) {
		unsigned prev = link;
		entry = table[link = next].key_loc_link;
		next = entry & linkmask;
		trace("entry = {0x%lx, %lx}", tri_third(&tri, entry), tri_second(&tri, entry));
		if ((entry & pairmask) == pairdata) {
			trace("free this");
			set_link(prev, next);
			set_link(link, free);
			free = link;
			count--;
			return 0;
		}
	}

	return -ENOENT;
}

#if 0
void shard::append_or_flatten(hashkey_t key, loc_t loc, cell_t flag)
{
	if (fifo.tail == fifo.top) {
		trace_on("flatten shard %u", ix);
		flatten();
		return;
	}
	append(tier().duo.pack(key, loc) | flag);
}
#endif

/* Provide bigmap externals */

u8 *ext_bigmap_mem(struct bigmap *map, loc_t loc)
{
	return map->rbspace + power2(map->blockbits, loc);
}

void ext_bigmap_map(struct bigmap *map, unsigned level, loc_t loc)
{
	if (loc == map->blocks) {
		if (map->blocks >= map->maxblocks)
			error_exit(1, "too many blocks (%u)", map->blocks + 1);
		map->blocks++;
	}
	map->path[level].map.loc = loc;
}

void ext_bigmap_unmap(struct bigmap *map, struct datamap *dm)
{
	dm->data = NULL;
	dm->loc = -1;
}

unsigned ext_bigmap_big(struct bigmap *map, struct datamap *dm)
{
	struct recinfo ri = {map->blocksize, map->reclen, dm->data};
	return ri.map->recops.big(&ri);
}

/* High level db ops */

struct delete_logent
{
	uint8_t logtype;
	uint8_t ax; // absolute tier index
	uint16_t ix; // shard index (hash high bits)
	uint32_t at; // entry index
	uint64_t duo; // media entry (hash, block)
};

struct insert_logent : delete_logent
{
	u8 keylen, unused[7];
};

struct unify_logent
{
	u8 data[32];
};

void keymap::showlog()
{
	struct pmblock *log = microlog;

	for (int i = 0; i < logsize; i++) {
		struct pmblock block;
		struct delete_logent entry;
		log_read(&block, log, i);
		memcpy(&entry, &block, sizeof entry); // stupid, but strict aliasing requires this!
		printf("%i.%i", i, entry.logtype);
		printf(" %lx %x %x %x", entry.duo, entry.ax, entry.ix, entry.at);
#ifdef SIDELOG
		struct sidelog *sidelog = (struct sidelog *)Private;
		struct sidelog side = sidelog[i];
		printf(" %lx %x %x %x", side.duo, side.rx, side.ix, side.at);
#endif
		printf("\n");
	}
}

void keymap::checklog(unsigned flags = 1)
{
	static unsigned bebug = 0;
	struct pmblock *log = microlog;

	if (logtail & ~logmask)
		error_exit(1, "%u: logtail out of range", bebug);

	if (loghead & ~logmask)
		error_exit(1, "%u: loghead out of range", bebug);

	if (flags & 1)
		for (int i = loghead, j = logtail; i != j; i = (i + 1) & logmask) {
			struct pmblock pmb;
			log_read(&pmb, log, i);

			struct delete_logent entry;
			memcpy(&entry, &pmb, sizeof entry);
#ifdef SIDELOG
			struct sidelog *sidelog = (struct sidelog *)Private;
			struct sidelog side = sidelog[i];

			if (entry.logtype == 1) {
				if (entry.duo != side.duo)
					goto corrupt;
			}
#endif
		}

	bebug++;
	return;

#ifdef SIDELOG
corrupt:
	error_exit(1, "%u: log corrupt", bebug);
#endif
}

int keymap::unify()
{
	enum {verify = 0};

	struct pmblock *log = microlog;
	trace("[%u] %i %i", id, loghead, burst());

	for (int i = loghead, j = logtail; i != j; i = (i + 1) & logmask) {
#ifdef SIDELOG
		struct sidelog *sidelog = (struct sidelog *)Private;
		struct sidelog side = sidelog[i];
		if (verify) {
			struct pmblock block;
			struct delete_logent entry;
			log_read(&block, log, i);
			memcpy(&entry, &block, sizeof entry); // stupid, but strict aliasing requires this!
			assert(sidelog[i].duo == entry.duo);
		}
		assert(tiers[side.rx].shardmap);
		if (1)
			tiers[side.rx].store(side.ix, side.at, side.duo);
#else
		struct pmblock block;
		struct insert_logent entry;
		log_read(&block, log, i);
		memcpy(&entry, &block, sizeof entry); // stupid or not, strict aliasing requires this!
		hashkey_t hash;
		loc_t loc;
		duo_unpack(&tiers[entry.ix].duo, entry.duo, hash, loc);

//		trace("%i: '%s' => %i:%u %.16lx @%i", i,
//			cprinz(&block + sizeof entry, entry.head.len),
		trace("%i: %.16lx @%i", i, hash, entry.at);
		if (1)
			tiers[entry.ix].store(entry.ix, entry.at, entry.duo);
		if (0)
			hexdump(&entry, linesize);
#endif
	}

	unsigned dirtylen = blocksize;
	trace_off("append %i bytes", dirtylen);
	assert(dirtylen <= blocksize);

	if (1) {
		struct unify_logent unify = {};
		log_commit(microlog, &unify, sizeof unify, &logtail);
	}

	if (1)
		pmwrite(rbspace + power2(blockbits, path[0].map.loc), path[0].map.data, dirtylen + (-dirtylen & linemask));
	if (1)
		pmwrite(upper->countmap, upper->countbuf, upper->shards() << countshift);

	if (0) {
		for (unsigned i = 0, n = std::min(upper->shards(), 50U); i < n; i++)
			if (upper->countbuf[i]) {
				printf("%i:\n", i);
				hexdump(upper->shardmap + power2(upper->stridebits - cellshift, i), power2(cellshift, upper->countbuf[i] + 1));
			}
	}

	sfence();

	loghead = logtail;
	return 0;
}

enum {verify = 0};

rec_t *keymap::insert(const void *key, unsigned keylen, const void *newrec, bool unique)
{
	assert(sizeof(struct insert_logent) == 24);

	cell_t hash = keyhash(key, keylen) & keymask;
	trace("insert %s => %lx", cprinz((const char *)key, keylen), hash);
	struct shard *shard = getshard(hash >> sigbits, 1);

	if (unique && shard->lookup(key, keylen, hash))
		return (rec_t *)errwrap(-EEXIST);

	if (1 && burst() == logsize - 1) { // one slot reserved for unify
		trace("log limit --> unify");
		unify();
	}

	while (1) {
		struct recinfo &ri = sinkinfo();
		if (verify)
			assert(!recops.check(&ri));
		rec_t *rec = (recops.create)(&ri, key, keylen, hash, newrec, 0);
		if (!is_errcode(rec)) {
			loc_t loc = path[0].map.loc;
			/*
			 * Many things can change in insert_and_grow:
			 *  - map can grow
			 *  - sigbits can change
			 *  - shard can be destroyed and replaced in map
			 *  - replacement can have different tier
			 *  - replacement can have different index
			 */
			insert_and_grow(shard, hash, loc); // error return!
			assert(shard == map[hash >> sigbits]); // super paranoia
			unsigned tx = shard->tx, ix = shard->ix;
			struct tier &tier = tiers[tx];
			cell_t duo = duo_pack(&tier.duo, hash & bitmask(sigbits), loc);
			unsigned at = tier.countbuf[ix]++, ax = tx ^ (upper - tiers);

			struct insert_logent head = { // this usage requires c++17
				{ .logtype = 1, .ax = ax, .ix = ix, .at = at, .duo = duo },
				.keylen = keylen };

			u8 logent[sizeof(struct pmblock)];
			memcpy(logent, &head, sizeof head);
			memcpy(logent + sizeof head, newrec, reclen);
			memcpy(logent + sizeof head + reclen, key, keylen);
			unsigned size = sizeof head + reclen + keylen;
#ifdef SIDELOG
			struct sidelog *sidelog = (struct sidelog *)Private;
			sidelog[logtail] = (struct sidelog){ .duo = duo, .at = at, .ix = ix, .rx = tx };
#endif
			log_commit(microlog, logent, size, &logtail);
			if (0)
				checklog(0);
			return rec;
		}

		if (0)
			recops.dump(&ri);

		assert(errcode(rec) == -ENOSPC);
//		trace("block full (%i of %i)", blocksize - recops.free(), blocksize);
		trace("block full");

		if (burst()) {
			trace("block full --> unify");
			unify();
		}

		if (bigmap_try(this, keylen, recops.big(&ri)) == 1)
			recops.init(&ri);
	}
}

int keymap::remove(const void *key, unsigned len)
{
	trace("delete '%.*s'", len, (const char *)key);
	hashkey_t hash = keyhash((const u8 *)key, len) & keymask;
	return getshard(hash >> sigbits, 1)->remove(key, len, hash); // wrong! could create a shard just to remove a nonexistent entry
}

int shard::remove(const void *key, unsigned len, hashkey_t hash)
{
	cell_t lowkey = hash & bitmask(lowbits);
	unsigned link = (hash >> lowbits) & bitmask(tablebits);
	loc_t loc;

	if (bucket_used(link)) {
		tests++;
		do {
			const cell_t &entry = table[link].key_loc_link;
			if (tri_third(&tri, entry) == lowkey) {
				loc = tri_second(&tri, entry);
				trace("probe block %x", loc);
				probes++;
				struct recinfo ri = map->peekinfo(loc);
				int err = map->recops.remove(&ri, key, len, hash);
				if (!err) {
					trace("delete %i/%i, big = %i", loc, len, map->recops.big(&ri));
					if (remove(hash, loc) == -ENOENT)
						break;
					bigmap_free(map, loc, map->recops.big(&ri));
					goto logging;
				}
			}
			link = next_entry(link);
		} while (link != endlist);
	}
	return -ENOENT;

logging:
	// don't forget: squash still not handled!!! (also need for insert)
	struct tier &tier = map->tiers[tx];
	cell_t duo = duo_pack(&tier.duo, hash & bitmask(map->sigbits), loc) | high64;
	unsigned at = tier.countbuf[ix]++, ax = tx ^ (map->upper - map->tiers);
	struct delete_logent head = { .logtype = 2, .ax = ax, .ix = ix, .at = at, .duo = duo };
#ifdef SIDELOG
	struct sidelog *sidelog = (struct sidelog *)map->Private;
	sidelog[map->logtail] = (struct sidelog){.duo = duo, .at = at, .ix = ix, .rx = tx};
#endif
	log_commit(map->microlog, &head, sizeof head, &map->logtail); // uninitialized at end!
	return 0;
}

int test(int argc, const char *argv[])
{
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

	int fd = open(argv[1], O_CREAT|O_RDWR, 0644);
	if (fd == -1)
		errno_exit(1);

	struct keymap sm{head, fd, fixsize::recops};

	enum {samesize = 0, maxkey = 255};
	u8 key[maxkey + (-maxkey & 7)];

	auto makekey = [&](int i) -> int
	{
		int keylen;

		if (1)
			keylen = uform((char *)key, sizeof key, i, 0x10);
		else if (1)
			keylen = snprintf((char *)key, sizeof key, samesize ? "%06x" : "%u", i);
		else
			memset(key, 'a' + (i & 0xf), keylen = 6);

		return keylen;
	};

	if (0) {
		trace_on("verify unique");
		u8 data[sm.reclen] = {};
		printf("result %p\n", sm.insert((const u8 *)"foo", 3, data));
		printf("result %p\n", sm.insert((const u8 *)"foo", 3, data));
		return 0;
	}

	unsigned n = argc > 2 ? atoi(argv[2]) : 10;

	if (1) {
		trace_on("%i inserts", n);
		u8 data[sm.reclen] = {};
		for (unsigned i = 0; i < n; i++) {
			int keylen = makekey(i);
			trace("%i: insert '%.*s'", i, keylen, key);
			if (sm.reclen >= sizeof(u32))
				((u32 *)data)[0] = i;
			sm.insert(key, keylen, data);
		}
	}

	if (0) {
		trace_on("%i lookups", n);
		sm.unify();
		for (unsigned i = 0; i < n; i++) {
			int keylen = makekey(i);
			trace("%i: lookup '%.*s'", i, keylen, key);
			rec_t *rec = sm.lookup(key, keylen);
			if (!rec)
				warn("lookup '%.*s' failed", keylen, key);
			if (0 && rec)
				hexdump(rec, 6); // use per table reclen!!!
		}
	}

	if (1) {
		char *key = (char *)"123";
		rec_t *rec = sm.lookup(key, strlen(key));
		if (!rec)
			return 1;
		hexdump(rec, 6); // use per table reclen!!!
		printf("remove result: %i\n", sm.remove(key, strlen(key)));
		printf("remove result: %i\n", sm.remove(key, strlen(key)));
		printf("lookup result: %p\n", sm.lookup(key, strlen(key)));
		sm.dump(4);
	}

	if (0)
		sm.grow_map(1);

	if (0)
		sm.getshard(0)->dump(-1, "");

	if (0)
		sm.dump(4);

	if (0) {
		sm.unify();
		struct shard *oldshard = sm.map[0];
		oldshard->dump(4, "old: ");
		sm.map[0] = 0;
		struct shard *newshard = sm.populate(0);
		newshard->dump(4, "new: ");
		unsigned missing = oldshard->count;
		newshard->walk(
			[&oldshard, &missing](hashkey_t key, loc_t loc)
			{ missing -= !oldshard->remove(key, loc); });
		oldshard->dump(4, "old: ");
		printf("missing %i\n", missing);
	}

	if (0) {
		enum {verbose = 0};

		struct context { int count, reclen; } context = { 0, sm.reclen };

		auto actor = [](void *context_, u8 *key, unsigned keylen, u8 *data, unsigned reclen)
		{
			struct context *context = (struct context *)context_;

			if (verbose) {
				printf("%.*s: ", keylen, key);
				hexdump(data, std::min(reclen, 16U));
			}
			context->count++;
		};

		if (1)
			sm.unify();

		trace_on("walk %i blocks", sm.blocks);
		for (loc_t loc = 0; loc < sm.blocks; loc++) {
			if (!is_maploc(loc, sm.blockbits)) {
				trace_off("block %i", loc);
				struct recinfo ri = sm.peekinfo(loc);
				sm.recops.walk(&ri, actor, &context);
			}
		}
		trace_on("found %i entries", context.count);
	}

	if (0) {
		int fd = open("foo", O_CREAT|O_RDWR, 0644);
		struct keymap map{head, fd, fixsize::recops, 6};
		u8 data[map.reclen] = {};
		map.insert("foo", 3, data);
		map.dump(4);
	}

	if (0) {
		trace_on("verify unique");
		u8 data[sm.reclen] = {};
		printf("result %p\n", sm.insert((const u8 *)"foo", 3, data));
		printf("result %p\n", sm.insert((const u8 *)"foo", 3, data));
		return 0;
	}

	if (1) {
		trace_on("%i inserts", n);
		u8 data[sm.reclen] = {};
		for (unsigned i = 0; i < n; i++) {
			int keylen = makekey(i);
			trace("%i: insert '%.*s'", i, keylen, key);
			if (sm.reclen >= sizeof(u32))
				((u32 *)data)[0] = i;
			sm.insert(key, keylen, data);
		}
	}

	if (0) {
		trace_on("%i lookups", n);
		sm.unify();
		for (unsigned i = 0; i < n; i++) {
			int keylen = makekey(i);
			trace("%i: lookup '%.*s'", i, keylen, key);
			rec_t *rec = sm.lookup(key, keylen);
			if (!rec)
				warn("lookup '%.*s' failed", keylen, key);
			if (0 && rec)
				hexdump(rec, 6); // use per table reclen!!!
		}
	}

	if (1) {
		char *key = (char *)"123";
		rec_t *rec = sm.lookup(key, strlen(key));
		if (!rec)
			return 1;
		hexdump(rec, 6); // use per table reclen!!!
		printf("remove result: %i\n", sm.remove(key, strlen(key)));
		printf("remove result: %i\n", sm.remove(key, strlen(key)));
		printf("lookup result: %p\n", sm.lookup(key, strlen(key)));
		sm.dump(4);
	}

	if (0)
		sm.grow_map(1);

	if (0)
		sm.getshard(0)->dump(-1, "");

	if (0)
		sm.dump(4);

	if (0) {
		sm.unify();
		struct shard *oldshard = sm.map[0];
		oldshard->dump(4, "old: ");
		sm.map[0] = 0;
		struct shard *newshard = sm.populate(0);
		newshard->dump(4, "new: ");
		unsigned missing = oldshard->count;
		newshard->walk(
			[&oldshard, &missing](hashkey_t key, loc_t loc)
			{ missing -= !oldshard->remove(key, loc); });
		oldshard->dump(4, "old: ");
		printf("missing %i\n", missing);
	}

	if (0) {
		enum {verbose = 0};

		struct context { int count, reclen; } context = { 0, sm.reclen };

		auto actor = [](void *context_, u8 *key, unsigned keylen, u8 *data, unsigned reclen)
		{
			struct context *context = (struct context *)context_;

			if (verbose) {
				printf("%.*s: ", keylen, key);
				hexdump(data, std::min(reclen, 16U));
			}
			context->count++;
		};

		if (1)
			sm.unify();

		trace_on("walk %i blocks", sm.blocks);
		for (loc_t loc = 0; loc < sm.blocks; loc++) {
			if (!is_maploc(loc, sm.blockbits)) {
				trace_off("block %i", loc);
				struct recinfo ri = loc == sm.path[0].map.loc ? sm.sinkinfo() : sm.peekinfo(loc);
				sm.recops.walk(&ri, actor, &context);
			}
		}
		trace_on("found %i entries", context.count);
	}

	if (0) {
		int fd = open("foo", O_CREAT|O_RDWR, 0644);
		struct keymap map{head, fd, fixsize::recops, 6};
		u8 data[map.reclen] = {};
		map.insert("foo", 3, data);
		map.dump(4);
	}

	//printf("used %i blocks\n", sink.block + 1);
	printf("tests %li probes %li blocks %u\n", tests, probes, sm.blocks);
	return 0;
}
