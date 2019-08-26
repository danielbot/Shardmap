#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "debug.h"

enum {check = 0, max_len = 255};

#define trace trace_off

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;
typedef u32 loc_t;

#include "bigmap.h"

struct recinfo { void *data; unsigned size, recsize; }; // defined in multiple places!

#if 0
// this will replace ext_bigmap_map after initial integration kvs is done
// it seems, datamap loc is never needed by code, only in mapping fns
// to avoid a remap when already mapped to desired loc
u8 *bmap(struct bigmap *map, loc_t loc, struct datamap *dm)
{
	return NULL;
}
#endif

/*
 * Record free space management for Shardmap
 * Copyright (c) Daniel Phillips, 2016-2019
 * License: GPL v3
 */

static loc_t ith_to_maploc(unsigned level, unsigned blockbits, unsigned stridebits, unsigned ith)
{
	return level + (!ith && level > 1 ? (1 << (stridebits - blockbits)) : ith << stridebits);
}

static unsigned bigmap_wrap(struct bigmap *map, unsigned stridebits, unsigned ith)
{
	unsigned blocksize = 1 << map->blockbits;
	unsigned last = map->blocks >> stridebits;
	if (ith < last)
		return blocksize;
	unsigned subbits = stridebits - map->blockbits, submask = (1 << subbits) - 1;
	return ((map->blocks + submask) >> subbits) - (last << map->blockbits);
	return ((map->blocks >> subbits) & (blocksize - 1)) + !!(map->blocks & submask);
}

unsigned add_map_level(struct bigmap *map)
{
	unsigned level = map->levels, blocksize = map->blocksize;
	void *frontbuf = aligned_alloc(blocksize, blocksize);
	map->path[map->levels++] = (struct level){.map = {frontbuf}}; // check too many levels!!!
	trace("new map level %u", level);
	return level;
}

static void level_load(struct bigmap *map, unsigned level, loc_t loc, unsigned wrap)
{
	trace("path[%i] => %i (wrap %i)", level, loc, wrap);
	ext_bigmap_map(map, level, loc);
	struct level *mapping = &map->path[level];
	trace_off("loc %i %i", loc, mapping->map.loc);
	mapping->start = mapping->at = mapping->big = 0;
	mapping->wrap = wrap;
}

void add_new_rec_block(struct bigmap *map)
{
	level_load(map, 0, map->blocks, 0);
}

static void add_new_map_block(struct bigmap *map, unsigned level, u8 init[], unsigned bytes)
{
	level_load(map, level, map->blocks, bytes);
	u8 *data = map->path[level].map.data;
	memset(data, 0, 1 << map->blockbits);
	memcpy(data, init, bytes);
}

void bigmap_load(struct bigmap *map, loc_t loc)
{
	if (map->path[0].map.loc != loc) {
		trace("load dir block %u", loc);
		level_load(map, 0, loc, 0);
	}
}

static void mapblock_load(struct bigmap *map, unsigned level, unsigned ith, unsigned stridebits)
{
	loc_t loc = ith_to_maploc(level, map->blockbits, stridebits, ith);
	if (map->path[level].map.loc != loc) {
		trace("load map block %u", loc);
		level_load(map, level, loc, 0);
	}
	map->path[level].wrap = bigmap_wrap(map, stridebits, ith);
}

/*
 * Determine whether a particular location holds a map block or not.
 *
 * This relies on the fact that, if a map block for a higher level is present,
 * then map blocks for all the sublevels are present immediately below it.
 * The lowest level map block, level 1, always appears at offset 1 modulo the
 * lowest level stride. So the offset of a map block modulo the lowest level
 * stride (bytes per block) is equal to the map level.
 *
 * A block at level zero is never a map block, and at level one is always a map
 * block. For higher levels, the block at offset equal to the level within the
 * stride for that level is a map block, except for the first map block at that
 * level, which appears immediately above the second map block of the level
 * below.
 *
 * So, first determine the level that a block must be if it is a map block,
 * trivially giving the result for levels zero and one. Then to avoid undefined
 * shift behavior, determine that impossibly high levels cannot be map blocks.
 * The general case has three parts: if the lowest map block of the level is
 * above the location then the block is not a map block, otherwise, check
 * whether the block is exactly the lowest map block of a level, otherwise,
 * check whether the block is at the expected offset at the base of its stride.
 */
bool is_maploc(loc_t loc, unsigned blockbits)
{
	unsigned stride = 1 << blockbits, level = loc & (stride - 1);

	switch (level) {
		case 0:
			return 0;
		case 1:
			return 1;
	}

	unsigned stridebits = blockbits * (level - 2);
	if (stridebits >= 32 - blockbits)
		return 0;
	stride <<= stridebits;

	return
		loc < stride ? 0 :
		loc == stride + level ? 1 :
		(loc & ((stride << blockbits) - 1)) == level;
}

/*
 * Determine the next data block location above a given data block,
 * skipping any map blocks.
 *
 * Note: this assumes that the input location is a data block, not a map
 * block, so user input such as a seek must be validated.
 */
static loc_t nextloc(unsigned blockbits, loc_t loc)
{
	unsigned stridebits = blockbits;

	for (unsigned level = 1; level < bigmap_maxlevels; level++, stridebits += blockbits) {
		unsigned stride = 1ULL << stridebits;
		if (++loc < stride) {
			if ((loc >> (stridebits - blockbits)) != 1)
				break;
		} else
			if ((loc & (stride - 1)) != level)
				break;
	}
	return loc;
}

static unsigned maplevels(unsigned blocks, unsigned blockbits) // log base 2**blockbits
{
	trace_off("blocks %u blockbits %u", blocks, blockbits);
	if (0 && !blocks) // hmm.
		return 0;
	loc_t stride = 1;
	for (unsigned level = 1; level < bigmap_maxlevels; level++, stride <<= blockbits)
		if (blocks <= stride)
			return level;
	assert(0);
	return 0;
}

size_t bigmap_check(struct bigmap *map)
{
	unsigned blockbits = map->blockbits, blocks = map->blocks, blocksize = 1 << blockbits;
	unsigned levels = maplevels(blocks, blockbits);
	size_t slack = 0;
	for (unsigned level = 1, stridebits = blockbits; level < levels; level++, stridebits += blockbits) {
		unsigned stride = 1 << stridebits, maps = (blocks + stride - 1) >> stridebits;
		for (unsigned i = 0; i < maps; i++) {
			trace_off("check %u level %u", i, level);
			unsigned wrap = bigmap_wrap(map, stridebits, i);
			trace_off("load map %u wrap %u", maploc, wrap);
			struct datamap parent = {.data = ext_bigmap_mem(map, i), .loc = i};

			for (unsigned j = 0; j < wrap; j++) {
				unsigned child_ith = (i << blockbits) + j;
				loc_t m = ith_to_maploc(level - 1, blockbits, stridebits - blockbits, child_ith);
				struct datamap child = {.data = ext_bigmap_mem(map, m), .loc = m};
				unsigned big = 0;
				trace_off("load %u", child_loc);
				if (level > 1) {
					for (unsigned k = 0; k < blocksize; k++)
						if (big < child.data[k])
							big = child.data[k];
				} else {
					if (is_maploc(child.loc, blockbits)) {
						if (parent.data[j]) {
							printf("=== level %u %u[%u]:%u should be zero but is %u\n",
								level, parent.loc, j, child.loc, parent.data[j]);
							continue;
						}
					} else {
						big = ext_bigmap_big(map, &child);
					}
				}
				if (parent.data[j] < big)
					printf("=== level %u %u[%u]:%u (%u < %u)\n",
						level, parent.loc, j, child.loc, parent.data[j], big);
				else
					slack += parent.data[j] - big;
				ext_bigmap_unmap(map, &child);
			}
			ext_bigmap_unmap(map, &parent);
		}
	}
	return slack;
}

void bigmap_dump(struct bigmap *map)
{
	unsigned blockbits = map->blockbits, blocksize = 1 << blockbits;
	for (loc_t i = 0; i < map->blocks;) {
		loc_t next = nextloc(blockbits, i);
		struct datamap dm = {.data = ext_bigmap_mem(map, i), .loc = i};
		printf("%u: %u\n", i, ext_bigmap_big(map, &dm));
		for (unsigned m = i + 1, level = 1; m < next; m++, level++) {
			unsigned stridebits = level * blockbits;
			unsigned wrap = bigmap_wrap(map, stridebits, i >> stridebits);
			int in_path = m == map->path[level].map.loc;
			struct datamap mb = {.data = ext_bigmap_mem(map, m), .loc = m};
			printf("%u:", m);
			for (unsigned j = 0; j < blocksize; j++) {
				if (j >= wrap) {
					printf(" -");
					continue;
				}
				printf(" %s%s%u:%u",
					m == in_path && j == map->path[level].start ? "*" : "",
					m == in_path && j == map->path[level].at ? "@" : "",
					ith_to_maploc(level - 1, blockbits, stridebits - blockbits, ((i >> stridebits) << blockbits) + j),
					mb.data[j]);
			}
			printf("\n");
			ext_bigmap_unmap(map, &mb);
		}
		ext_bigmap_unmap(map, &dm);
		i = next;
	}
}

static void path_check(struct bigmap *map)
{
	unsigned levels = map->levels, blockbits = map->blockbits;
	for (unsigned level = 1, stridebits = blockbits; level < levels; level++, stridebits += blockbits) {
		struct level *p = map->path + level;
		unsigned ith = p->map.loc >> stridebits;
		unsigned child_loc = ith_to_maploc(level - 1, blockbits, stridebits - blockbits, (ith << blockbits) + p->at);
		if (child_loc != map->path[level - 1].map.loc)
			printf("=== level %u %u[%u] maps %u but %u is loaded\n", level, p->map.loc, p->at, child_loc, map->path[level - 1].map.loc);
		unsigned wrap = bigmap_wrap(map, stridebits, ith);
		for (unsigned j = p->start; j != p->at; j = j + 1 == wrap ? 0 : j + 1) {
			if (p->map.data[j] > p->big)
				printf("=== map[%u] = %u, greater than %u\n", j, p->map.data[j], p->big);
		}
	}
}

static void path_dump(struct bigmap *map)
{
	for (unsigned level = 0; level < map->levels; level++) {
		struct level *p = map->path + level;
		printf("[%u] loc %u", level, p->map.loc);
		if (level)
			printf(" start %u at %u wrap %u big %u", p->start, p->at, p->wrap, p->big);
		printf("\n");
	}
	printf("[%u] big %u\n", map->levels, map->big);
}

static void path_load(struct bigmap *map, unsigned loc)
{
	unsigned levels = map->levels, blockbits = map->blockbits, ith = loc;
	unsigned blockmask = (1 << blockbits) - 1;
	for (unsigned level = 1, stridebits = blockbits; level < levels; level++, stridebits += blockbits) {
		unsigned at = ith & blockmask;
		mapblock_load(map, level, ith >>= blockbits, stridebits);
		map->path[level].start = map->path[level].at = at;
	}
	map->partial_path = 0;
	if (check)
		path_check(map);
}

static void set_sentinel(struct bigmap *map)
{
	map->path[map->levels] = (struct level){ .map.data = &map->big  };
	map->big = max_len;
}

static void map_new_block(struct bigmap *map)
{
	trace("add block");
	loc_t loc = map->blocks;
	add_new_rec_block(map);

	/* add map block or update len per level */
	unsigned blockbits = map->blockbits, stridebits = blockbits, blocksize = 1 << blockbits;
	unsigned newblocks = nextloc(blockbits, loc) - loc, newcount = 1; // paranoia check
	assert(newblocks <= blocksize);
	for (unsigned level = 1; level < map->levels; level++, stridebits += blockbits) {
		struct level *p = map->path + level;
		unsigned stridemask = (1 << stridebits) - 1;
		if ((loc & stridemask) == 0) {
			add_new_map_block(map, level, (u8[]){max_len}, 1);
			if (level == 1)
				p->wrap = newblocks;
			trace("new map block %u at level %u", p->map.loc, level);
			newcount++;
		} else {
			unsigned ith = loc >> stridebits;
			unsigned rightmost = (loc >> (stridebits - blockbits)) & (blocksize - 1);
			unsigned wrap = bigmap_wrap(map, stridebits, ith);
			loc_t loc = ith_to_maploc(level, map->blockbits, stridebits, ith);
			if (map->path[level].map.loc != loc) {
				trace("load map block %u", loc);
				level_load(map, level, loc, wrap);
				p->start = p->at = rightmost;
			} else {
				assert(p->at == p->start);
				p->start = 0;
				p->at = rightmost;
				p->wrap = wrap;
			}
			p->map.data[p->at] = max_len;
			trace("update %u[%u] level %u", p->map.loc, p->at, level);
		}
		assert(p->wrap);
	}

	/* add new map level? */
	if (loc == 1 << (stridebits - map->blockbits)) {
		unsigned level = add_map_level(map);
		unsigned big = map->big;
		add_new_map_block(map, level, (u8[]){big, max_len}, 2);
		map->path[level].at = 1;
		map->path[level].big = big;
		//bigmap_dump(map);
		//path_dump(map);
		if (check)
			bigmap_check(map);
		set_sentinel(map);
		newcount++;
	}

	assert(newcount == newblocks);
	map->big = max_len;
}

/*
 * Returns with path level zero mapped to a block that may have at least
 * enough free space to store a record with key of indicated length. Path
 * levels 1 and higher are mapped consistently at entry and exit, so that
 * parent blocks do not need to be reloaded on pop to higher path level
 * and cached scan state avoids rescan on repeated searches.
 *
 * It is not certain that the returned block can actually store the name
 * record, due to possible high cost of determining biggest remaining free
 * per create, therefore it is possible that every block might need to be
 * scanned in one search series, however scanning any block more than once
 * would indicate a problem, not detected, but should be.
 *
 * Why do we pass in big, the biggest possible key that can be created in
 * this block? Because caller only searches for a block with free space
 * after an attempted create has already failed, so it may already know
 * the exact remaining free space, to avoid future futile create attempts.
 * Or it can assume len - 1 if impractical to know exactly.
 */
int bigmap_try(struct bigmap *map, unsigned len, unsigned big)
{
	// call ext_bigmap_big right here instead of passing in!!!
	trace("blocks %u blockbits %u levels %u %p", map->blocks, map->blockbits, map->levels, map);
	trace_off("maplevels %u\n", maplevels(map->blocks, map->blockbits));
	assert(map->levels == maplevels(map->blocks, map->blockbits));
	assert(len > big);
	struct level *p = map->path + 1;

	if (map->levels <= 1) {
		if (map->levels == 0) {
		}

		trace("add first map block");
		add_new_map_block(map, add_map_level(map), (u8[]){big, 0, max_len}, 3);
		add_new_rec_block(map);
		p->big = big;
		p->at = 2;
		set_sentinel(map);
		return 0;
	}

	trace("find free levels %u big %u", map->levels, big);

	unsigned blockbits = map->blockbits;
	unsigned level = 1, stridebits = blockbits;

	if (map->partial_path)
		path_load(map, map->path[0].map.loc);

	p->map.data[p->at] = big;
	assert(len > p->map.data[p->at]);

	while (1) {
		trace("find free level %u", level);
		assert(p == map->path + level);
		if (0) {
			bigmap_dump(map);
			path_dump(map);
		}
		if (check)
			bigmap_check(map);

		/* if len less than already seen then rescan */
		if (p->big >= len) {
			p->at = p->start;
			p->big = 0;
		}

		while (1) {
			unsigned bound = p->map.data[p->at];
			assert(p == map->path + level && stridebits == level * blockbits);
			trace("scan %u:%u[%u] = %u, wrap %u, big %u", level, p->map.loc, p->at, bound, p->wrap, p->big);

			if (len <= bound) {
				/* probe subtree */
				unsigned ith = ((p->map.loc >> stridebits) << blockbits) + p->at;

				trace("push to %u[%u] ith %u", p->map.loc, p->at, ith);
				level--, p--;
				if (!level) {
					bigmap_load(map, ith);
					if (check)
						path_check(map);
					return 0;
				}
				stridebits -= blockbits;
				mapblock_load(map, level, ith, stridebits);
				continue;
			}

			if (p->big < bound)
				p->big = bound;

			if (++p->at == p->wrap)
				p->at = 0;

		    if (p->at == p->start)
				break;
		}

		trace("scanned entire block and failed, big = %u", p->big);
		/* set parent to actual biggest and backtrack to parent level */
		assert(p[1].at == ((p->map.loc >> stridebits) & ((1 << blockbits) - 1)));
		p[1].map.data[p[1].at] = p->big;
		p++, level++;
		if (level == map->levels) {
			map_new_block(map);
			assert(ext_bigmap_big(map, &map->path[0].map) >= len);
			if (check)
				path_check(map);
			return 0;
		}
		stridebits += blockbits;
	}
}

int bigmap_free(struct bigmap *map, loc_t loc, unsigned big)
{
	unsigned blockbits = map->blockbits;
	unsigned blockmask = (1 << blockbits) - 1;
	unsigned levels = map->levels, ith = loc;
	for (unsigned level = 1, stridebits = blockbits; level < levels; level++, stridebits += blockbits) {
		unsigned at = ith & blockmask;
		ith >>= blockbits;
		mapblock_load(map, level, ith, stridebits);
		struct level *p = map->path + level;
		if (p->map.data[at] >= big) {
			map->partial_path = 1;
			return 0;
		}
		trace("%i@%i was %i", p->map.loc, at, p->map.data[at]);
		p->map.data[at] = big;
		p->start = p->at = at;
	}
	map->big = big;
	return 0;
}

void bigmap_open(struct bigmap *map)
{
	trace("open map, levels %u", map->levels);
	map->levels = maplevels(map->blocks, map->blockbits);
	for (unsigned level = 0; level < map->levels; level++)
		map->path[level] = (struct level){ .map.loc = -1 };
	trace("blocks %i levels %i", map->blocks, map->levels);
	set_sentinel(map);
//	level_load(map, 0, 0, 0); // load record block 0, is this right???
}

void bigmap_close(struct bigmap *map)
{
	for (unsigned level = 0; level < map->levels; level++)
		ext_bigmap_unmap(map, &map->path[level].map);
}
