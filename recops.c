/*
 * Record block primitives for Shardmap
 * Copyright (c) Daniel Phillips, 2016-2019
 * License: GPL v3
 *
 * gcc -c -Wall shardlib.c
 * g++ -Wall shardmap.cc shardlib.o && ./a.out
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

enum { cleanup = 0, cleaned = 0xee, deleted = 0xdd, holecode = 0xff };

#define trace trace_off

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint8_t u8;

static u8 rb_hash(u16 ihash) { return ihash % 255; }

struct recinfo { void *data; unsigned size, reclen; }; // defined in multiple places!

#include "debug.h"
#include "recops.h"

struct rb
{
	u16 size; /* blocksize */
	u16 used; /* entry headers and key text stored compactly at top of block */
	u16 free; /* total key text space in free entries within entry region */
	u16 count; /* total entries including free entries */
	u16 holes; /* number of free entries created by delete */
	char magic[2];
	struct tabent { u8 hash; u8 len; } table[];
};

enum {tabent_size = sizeof(struct tabent)};
enum {header_size = sizeof(struct rb) };

static inline struct rb *irb(struct recinfo ri)
{
	struct rb *rb = ri.data;
	assert(!memcmp(rb->magic, "DE", 2));
	//assert(ri.reclen == rb->reclen); // might add this field for redundancy
	return rb;
}

static unsigned rb_gap(struct rb *rb)
{
	return (void *)rb + rb->size - rb->used - (void *)(rb->table + rb->count);
}

/* Exports */

void rb_init(struct recinfo ri)
{
	*(struct rb *)ri.data = (struct rb){.size = ri.size, .magic = {"DE"}};
}

int rb_big(struct recinfo ri)
{
	struct rb *rb = irb(ri);
	unsigned reclen = ri.reclen;
	unsigned overhead = reclen + tabent_size;
	unsigned gap = rb_gap(rb);
	unsigned big = rb->holes ? gap + rb->free : (gap > overhead ? gap - overhead : 0);
	return big > maxname ? maxname : big;
}

int rb_free(struct recinfo ri)
{
	struct rb *rb = irb(ri);
	return rb_gap(rb) + rb->free;
}

void rb_dump(struct recinfo ri)
{
	struct rb *rb = irb(ri);
	unsigned reclen = ri.reclen;

	if (1)
		printf("%u entries: ", rb->count);
	rec_t *rec = ri.data + rb->size;
	char sep = 1 ? ' ' : '\n';
	for (unsigned i = 0; i < rb->count; i++) {
		unsigned len = rb->table[i].len;
		rec -= reclen + len;
		if (0)
			printf("%u\n", len);
		if (rb->table[i].hash == holecode)
			printf("(%u)%c", len, sep);
		else
//			printf("%.*s:%u%c", len, rec + ri.reclen, *(u32 *)rec, sep);
			printf("%x.%u:%u%c", rb->table[i].hash, len, *(u32 *)rec, sep);
	}
	printf("gap %i free %i holes %i\n", rb_gap(rb), rb->free, rb->holes);
}

/* Look up entry by index for testing, later use for seekdir */
void *rb_key(struct recinfo ri, unsigned which, unsigned *ret) // untested!
{
	struct rb *rb = irb(ri);
	rec_t *rec = ri.data + rb->size;
	unsigned reclen = ri.reclen;
	if (which >= rb->count) {
		*ret = 0;
		return NULL;
	}
	for (unsigned i = 0; i < which; i++)
		rec = rec - (reclen + rb->table[i].len);
	unsigned len = *ret = rb->table[which].len;
	rec -= reclen + len;
	return rb->table[which].hash == holecode ? NULL : (rec + reclen);
}

bool rb_check(struct recinfo ri)
{
	struct rb *rb = irb(ri);
	unsigned reclen = ri.reclen;
	unsigned scan_entry_count = 0, scan_hole_count = 0, scan_hole_space = 0, scan_entry_space = 0;
	unsigned count = rb->count;
	unsigned max_entries = (rb->size - header_size) / (reclen + 1 + tabent_size);
	unsigned errs = 0;

	if (count > max_entries && ++errs) {
		printf("too many entries (%u)\n", count);
		count = max_entries;
	}

	rec_t *rec = ri.data + rb->size;
	void *table_top = rb->table + count;

	for (unsigned i = 0; i < count; i++) {
		unsigned len = rb->table[i].len;
		rec -= reclen + len;

		if ((void *)rec < table_top && ++errs) {
			printf("entries overlap table\n");
			break;
		}

		if (rb->table[i].hash == holecode) {
			scan_hole_space += len;
			scan_hole_count++;
		} else {
			scan_entry_space += len;
			scan_entry_count++;
		}
	}

	if (rb->holes != scan_hole_count && ++errs)
		printf("holes count (%u) wrong - found %u holes\n", rb->holes, scan_hole_count);

	if (rb->free > rb->used - reclen * count && ++errs)
		printf("free records (%u) more than total records (%u)\n",
			rb->free,
			rb->used - reclen * count);

	if (reclen * count + scan_entry_space != rb->used - rb->free && ++errs)
		printf("wrong entry space count (%u): %u entries with %u bytes, %u bytes in holes (%u)\n",
			rb->used,
			scan_entry_count,
			reclen * count + scan_entry_space,
			scan_hole_space,
			reclen * count + scan_entry_space + scan_hole_space);

	if (scan_hole_space != rb->free && ++errs)
		printf("wrong hole space count (%u): %u holes found with %u unused bytes\n",
			rb->free,
			scan_hole_count,
			scan_hole_space);

	return errs;
}

rec_t *rb_lookup(struct recinfo ri, const u8 *key, u8 len, u16 lowhash)
{
	struct rb *rb = irb(ri);
	rec_t *rec = ri.data + rb->size;
	unsigned reclen = ri.reclen;
	unsigned hash = rb_hash(lowhash);
	assert(hash != holecode);

	for (unsigned i = 0; i < rb->count; i++) {
		unsigned keylen = rb->table[i].len;
		rec = rec - (reclen + keylen);
		trace("hash %x %x len %u", hash, rb->table[i].hash, len);
		if (rb->table[i].hash == hash && keylen == len) {
			if (!memcmp(key, rec + reclen, len))
				return rec;
		}
	}
	return NULL;
}

rec_t *rb_create(struct recinfo ri, const u8 *key, u8 len, u16 lowhash, const void *data)
{
	struct rb *rb = irb(ri);
	unsigned reclen = ri.reclen;
	unsigned gap = rb_gap(rb), last = rb->count - 1, pos = last;
	rec_t *rec;
	trace("hash %x gap %u free %u", rb_hash(lowhash), gap, rb->free);

	if (gap >= reclen + len + tabent_size) {
		trace("fast path create");
		rb->used += reclen + len;
		rec = ri.data + rb->size - rb->used;
		pos = rb->count++;
		goto create;
	}

	/*
	 * Here we have already mostly computed the largest key/value that could fit
	 * in the block and caller probably wants to know that so the space could be
	 * used in future for some smaller record. So consider returning that as a
	 * a positive number, and use -ENOSPC only for exactly zero. So any nonzero
	 * return means the create failed without any changes. Negative returns need
	 * to be tested to determine whether it means exactly full or something more
	 * problematic. Reconsider all this in the light of errcode pointer wrapper.
	 */
	if (!rb->holes || gap + rb->free < len)
		return errwrap(-ENOSPC);

	/* walk backward in dict until enough hole space found */
	rec_t *last_re = ri.data + rb->size - rb->used;
	int need = len - gap, holespace = 0;
	rec = last_re;
	unsigned keylen;

	do {
		keylen = rb->table[pos].len;
		trace_off(rec, reclen + keylen);
		trace("-- holespace = %i need = %i pos = %i", holespace, need, pos);
		if (rb->table[pos].hash == holecode) {
			holespace += keylen;
			if (holespace >= need)
				break;
		} else if (pos == 0)
			return errwrap(-EIO); // reusable hole not found due to corruption or bug
		rec = rec + reclen + rb->table[pos--].len;
	} while (1);

	unsigned use_entry = pos;
	holespace -= keylen; // create new entry here using this header
	need = len - keylen;
	trace("==> create new entry at %u len %u keylen %u", pos, len, keylen);
	/*
	 * Found a free entry, there are now three cases:
	 *  1) need < 0: too small, move down
	 *  2) need > 0: too large, move up
	 *  3) need = 0: same size, no action
	 */
	if (need > 0) {
		/* Case 1: Step backward through records, moving records down and shrinking holes */
		int movedown = need - holespace;
		if (movedown < 0)
			movedown = 0;
		rb->used += movedown; /* decrease gap */
		trace("movedown = %i pos = %i need = %i gap = %i", movedown, pos, need, rb_gap(rb));
		rec = last_re;
		pos = last;

		do {
			keylen = rb->table[pos].len;
			trace_off(rec, reclen + keylen);
			trace("-- pos = %i movedown = %i need = %i", pos, movedown, need);
			if (pos == use_entry)
				break;
			if (rb->table[pos].hash == holecode) {
				rec_t *newent = rec - movedown;
				int newlen = keylen - need;
				if (newlen < 0)
					newlen = 0;
				unsigned shrink = keylen - newlen;
				trace("shrink hole from %i to %i", keylen, newlen);
				if (cleanup)
					memset(newent, cleaned, newlen);
				assert(newlen <= maxname);
				rb->table[pos].len = newlen;
				rb->free -= shrink;
				movedown += shrink;
				need -= shrink;
			} else
				memmove(rec - movedown, rec, reclen + keylen);
			rec = rec + reclen + keylen;
			pos--;
		} while (1);
		assert(len - keylen == movedown);
		rec = rec - movedown;
		goto reuse;
	}

	if (need < 0) {
		/* Case 2: Step forward through records, moving records up, expand the first hole found  */
		int moveup = -need;
		need = 0;
		rec_t *use_re = rec + moveup;
		if (use_entry < last) do {
			keylen = rb->table[++pos].len;
			rec = rec - (reclen + keylen);
			trace("++ holespace = %i len = %u moveup = %i pos = %i last = %i", holespace, keylen, moveup, pos, last);
			trace_off(rec, reclen + keylen);
			if (rb->table[pos].hash == holecode) {
				if (cleanup)
					memset(rec + reclen + keylen, cleaned, moveup);
				assert(rb->table[pos].len + moveup <= maxname);
				rb->table[pos].len += moveup;
				rb->free += moveup;
				moveup = 0;
				break;
			}
			memmove(rec + moveup, rec, reclen + keylen);
			if (pos == last)
				break;
		} while (1);

		if (pos == last) {
			if (cleanup)
				memset(ri.data + rb->size - rb->used, 0, moveup);
			rb->used -= moveup; /* increase gap */
		}

		trace("gap = %u", rb_gap(rb));
		trace("old len = %u", rb->table[use_entry].len);
		rec = use_re;
		pos = use_entry;
		keylen = rb->table[use_entry].len;
		goto reuse;
	}

	trace("exact fit!");
reuse:
	rb->free -= keylen;
	rb->holes--;
create:
	rb->table[pos] = (struct tabent){rb_hash(lowhash), len};
	memcpy(rec, data, ri.reclen);
	memcpy(rec + reclen, key, len);
	return rec;
}

int rb_delete(struct recinfo ri, const u8 *key, u8 len, u16 lowhash)
{
	struct rb *rb = irb(ri);
	rec_t *rec = ri.data + rb->size;
	unsigned reclen = ri.reclen;
	unsigned hash = rb_hash(lowhash);

	for (unsigned i = 0; i < rb->count; i++) {
		unsigned keylen = rb->table[i].len;
		rec = rec - (reclen + keylen);
		if (rb->table[i].hash == hash && keylen == len) {
			if (!memcmp(key, rec + reclen, len)) {
				if (cleanup)
					memset(rec + reclen, cleaned, keylen);
				rb->table[i].hash = holecode;
				rb->free += keylen;
				rb->holes++;
				if (i == rb->count - 1) {
					trace("--- trim %i ---", i); // need unit test to verify all trimmed
					do {
						keylen = rb->table[rb->count = i].len;
						rb->free -= keylen;
						rb->used -= reclen + keylen;
						rb->holes--;
						if (cleanup) {
							memset(rec, 0, reclen + keylen);
							rb->table[i] = (struct tabent){};
						}
						rec = rec + (reclen + keylen);
					} while (i-- && rb->table[i].hash == holecode);
				}
				return 0;
			}
		}
	}
	return -ENOENT;
}

typedef void (rb_walk_fn)(void *context, u8 *key, unsigned len, u8 *data);

int rb_walk(struct recinfo ri, rb_walk_fn fn, void *context)
{
	struct rb *rb = irb(ri);
	rec_t *rec = ri.data + rb->size;
	unsigned reclen = ri.reclen;

	for (unsigned i = 0; i < rb->count; i++) {
		unsigned len = rb->table[i].len;
		rec -= reclen + len;
		if (rb->table[i].hash != holecode)
			fn(context, rec + reclen, len, rec);
	}
	return 0;
}
