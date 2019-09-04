/*
 * Map free space in a big directory for O(log N) search
 * (c) 2017 Daniel Phillips
 * License: GPL v3
 *
 * gcc -O3 -Wall bigmap_test.c recops.o && ./a.out
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <limits.h>
#include <errno.h>
#include "hexdump.c"

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;
typedef u32 loc_t;

#include "bigmap.c"
#include "recops.c"

void errno_exit(unsigned exitcode)
{
	fprintf(stderr, "%s! (%i)\n", strerror(errno), errno);
	exit(exitcode);
}

void error_exit(unsigned exitcode, const char *reason, ...)
{
	va_list arglist;
	va_start(arglist, reason);
	vfprintf(stderr, reason, arglist);
	va_end(arglist);
	fprintf(stderr, "!\n");
	BREAK;
	exit(exitcode);
}

enum {reclen_default = sizeof(unsigned)};

u8 fakemem[1 << 20];

u8 *ext_bigmap_mem(struct bigmap *map, loc_t loc)
{
	u8 *mem = fakemem + (loc << map->blockbits);
	assert(mem < fakemem + sizeof fakemem);
	return mem;
}

void ext_bigmap_map(struct bigmap *map, unsigned level, loc_t loc)
{
	trace("map %i:%i", level, loc);

	map->path[level].map.loc = loc;
	map->path[level].map.data = ext_bigmap_mem(map, loc);

	if (loc == map->blocks) {
		if (map->blocks >= map->maxblocks)
			error_exit(1, "too many blocks (%u)", map->blocks + 1);
		if (!level) {
			rb_init((struct recinfo){map->path[0].map.data, map->blocksize, map->reclen});
		}
		map->blocks++;
	}
}

void ext_bigmap_unmap(struct bigmap *map, struct datamap *dm)
{
	dm->data = NULL;
	dm->loc = -1;
}

unsigned ext_bigmap_big(struct bigmap *map, struct datamap *dm)
{
	return rb_big((struct recinfo){dm->data, map->blocksize, map->reclen});
}

static unsigned bebug = 0;

unsigned ext_big(struct bigmap *map, struct datamap *dm)
{
	return rb_big((struct recinfo){dm->data, map->blocksize, map->reclen});
}

int bigmap_create(struct bigmap *map, unsigned char *name, unsigned len, void *data)
{
	bebug++;
	do {
		struct recinfo ri = {map->path[0].map.data, map->blocksize, map->reclen};
		trace("create '%.*s' len %u", len, name, len);
		rec_t *rec = rb_create(ri, name, len, 0x66, data, 0); // use actual hash!
		if (!is_errcode(rec)) {
			assert(!rb_check(ri));
			if (0)
				rb_dump(ri);
			return 0;
		}
		assert(errcode(rec) == -ENOSPC);
		bigmap_try(map, len, rb_big(ri));
	} while (1);
}

int bigmap_delete(struct bigmap *map, void *name, unsigned len)
{
	unsigned loc = map->path[0].map.loc;
	trace("--- loc %u", loc);
	struct rb *rb = (void *)map->path[0].map.data;
	struct recinfo ri = {(u8 *)rb, map->blocksize, map->reclen};
	if (0)
		rb_dump(ri);
	int err = rb_delete(ri, name, len, 0x66);
	if (err)
		return err;
	bebug++;
	trace("delete %u %i/%i, big = %i", bebug, loc, len, rb_big(ri));
	return bigmap_free(map, loc, rb_big(ri));
}

void dir_open(struct bigmap *map)
{
	map->blocks = 0;
	map->maxblocks = sizeof fakemem >> map->blockbits;
	ext_bigmap_map(map, 0, 0);
	bigmap_open(map);
}

int main(int argc, const char *argv[])
{
	if (1) {
		enum {maxlen = 5};
		struct bigmap map = (struct bigmap){ .blockbits = 6, .blocksize = 1 << 6, .reclen = reclen_default };
		dir_open(&map);

		for (unsigned cycle = 0; cycle < 1000; cycle++) {
			trace("cycle %u", cycle);
			unsigned blocks = map.blocks;

			for (unsigned i = 0; i < blocks; i++) {
				unsigned loc = rand() % blocks;
				if (is_maploc(loc, map.blockbits))
					continue;
				ext_bigmap_map(&map, 0, loc);
				struct recinfo ri = {map.path[0].map.data, map.blocksize, map.reclen};
				unsigned len;
				u8 *name = rb_key(ri, 0, &len);
				if (name) {
					trace("delete '%.*s' len %u [%u]", len, name, len, loc);
					bigmap_load(&map, loc);
					if (bigmap_delete(&map, name, len))
						printf("could not delete %.*s!!!\n", len, name);
					if (check)
						bigmap_check(&map);
				}
			}
			while (map.blocks == blocks) {
				unsigned char name[maxlen + 1];
				unsigned len = rand() % maxlen + 1;
				int numlen = snprintf((void *)name, sizeof(name), "%u", bebug);
				if (len > numlen)
					memset(name + numlen, bebug % 26 + 'A', len - numlen);
				if (bigmap_create(&map, name, len, &bebug))
					printf("could not create %.*s!!!\n", len, name);
				if (check)
					bigmap_check(&map);
				if (0)
					bigmap_dump(&map);
			}
		}
		if (0) {
			bigmap_dump(&map);
			path_dump(&map);
			path_check(&map);
		}
		printf("slack = %Lu after %u operations\n", (long long)bigmap_check(&map), bebug);
		if (0)
			for (unsigned loc = 0; loc < map.blocks; loc++)
				if (!is_maploc(loc, map.blockbits)) {
					ext_bigmap_map(&map, 0, loc);
					struct recinfo ri = {map.path[0].map.data, map.blocksize, map.reclen};
					printf("%u: ", loc);
					rb_dump(ri);
				}
		return 0;
	}

	if (1) {
		struct bigmap map = (struct bigmap){ .blocks = -1, .blockbits = argc <= 1 ? 12 : atoi(argv[1])};
		for (loc_t i = 0; i <= map.blocks;) {
			loc_t j = nextloc(map.blockbits, i);
			if (i + 1 == j)
				if (is_maploc(j, map.blockbits))
					printf("0x%x?\n", j);
			for (loc_t k = i + 1; k < j; k++) {
				if (!is_maploc(k, map.blockbits))
					printf("0x%x!\n", k);
			}
			i = j;
		}
		return 0;
	}

	if (0) {
		/*
		 * Show map layout per layout and map path per block
		 */
		loc_t blocks = 70, blockbits = 2;
		printf("map layout for %i record blocks, blocksize = %i...\n", blocks, 1 << blockbits);
		unsigned blockmask = (1 << blockbits) - 1;
		unsigned levels = maplevels(blocks, blockbits);
		for (unsigned level = 1; level < levels; level++) {
			loc_t stride = 1 << level * blockbits;
			loc_t loc = level > 1 ? (1 << (level - 1) * blockbits) : 0, j = 1;
			printf("level %i stride %i: %i", level, stride, loc + level);
			assert(ith_to_maploc(level, blockbits, level * blockbits, 0) == loc + level);
			for (loc = stride; loc < blocks; loc += stride, j++) {
				printf(" %i", loc + level);
				assert(ith_to_maploc(level, blockbits, level * blockbits, j) == loc + level);
			}
			printf("\n");
		}
		printf("\nmap path per record block...\n");
		for (unsigned loc = 0; loc < blocks; loc = nextloc(blockbits, loc)) {
			printf("%i:", loc);
			unsigned ith = loc;
			for (
				unsigned level = 1, stridebits = blockbits;
				level < levels;
				level++, stridebits += blockbits)
			{
				unsigned at = ith & blockmask;
				ith >>= blockbits;
				printf(" %i@%i", ith_to_maploc(level, blockbits, stridebits, ith), at);
			}
			printf("\n");
		}
		return 0;
	}

	if (0) {
		struct bigmap map = (struct bigmap){ .blocks = 68, .blockbits = 2 };
		for (loc_t i = 0; i <= map.blocks; i++)
			if (is_maploc(i, map.blockbits))
				//;
				printf("%u ", i);
		printf("\n");
		return 0;
	}

	if (0) {
		enum { dirblocks = 64, blockbits = 2 };
		printf("blockbits %i blocks:levels:", blockbits);
		for (loc_t i = 0; i <= dirblocks; i++)
			printf(" %i:%i", i, maplevels(i, 2));
		printf("\n");
		return 0;
	}

	if (0) {
		enum { dirblocks = 75, blockbits = 2 };
		printf("walk non-map blocks for first %i dir blocks, blocksize = %i\n", dirblocks, 1 << blockbits);
		for (loc_t i = 0; i < dirblocks; i = nextloc(blockbits, i))
			printf("%i ", i);
		printf("\n");
		return 0;
	}

	if (0) {
		enum { dirblocks = 10, blockbits = 2 };
		for (loc_t i = 0; i < dirblocks; i++)
			printf("%i -> %i\n", i, nextloc(blockbits, i));
		return 0;
	}

	return 0;
}
