/*
 * Persistent memory logging operations
 * Daniel Phillips, 2019
 * License: GPL v3
 *
 * gcc -O3 -c -Wall shardlib.c
 * gcc -O3 -c -Wall pmem.c -Wno-unused-variable -Wno-unused-function
 * g++ -O3 -Wall shardmap.cc shardlib.o pmem.o && ./a.out /run/daniel/foo 1000
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include "debug.h"
#include "pmem.h"

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef unsigned char u8;
typedef unsigned fixed8;

#define trace trace_off

enum {debug = 0, verify = 0};

/* Persistent memory */

void pmwrite(void *to, void *from, unsigned len)
{
	if (verbose)
		printf("to %p from %p len %i\n", to, from, len);
	if (streaming) {
		if (1)
		    for (cell_t *s = from, *d = to, *top = to + len; d < top; s++, d++)
				ntstore64(d, *s);
		else
		    for (void *s = from, *d = to, *top = to + len; d < top; s += cellsize, d += cellsize)
				ntstore64((cell_t *)d, *(cell_t *)s);
	} else {
		memcpy(to, from, len);
		for (void *dirty = to; dirty < to + len; dirty += linesize)
			clwb(dirty);
	}

	if (0)
		hexdump(to, len);
}

/* Microlog */

void log_commit(struct pmblock log[logsize], void *data, unsigned len, unsigned *tail)
{
	unsigned cells = (len + (-len & 7)) >> cellshift;
	unsigned i = *tail, tag = (i >> logorder) & 3;

	if (verbose)
		printf("commit %i cells at [%i]\n", cells, i);

	assert(!(i & ~logmask));
	*tail = (i + 1) & logmask;
	cell_t *ram = data, *mem = log[i].data, savebits = 0;
	for (unsigned cell = 0, shift = 0x3e; cell < cells; cell++, shift -=2)
		savebits |= (ram[cell] & 3) << shift;
	for (unsigned cell = 0; cell < cells; cell++)
		mem[cell] = (ram[cell] & ~3) | tag;
	mem[blockcells - 1] = savebits | tag;
	for (unsigned cell = 0; cell < blockcells; cell += linecells)
		clwb(&mem[cell]);

	sfence();

	if (0)
		hexdump(log[i].data, sizeof (struct pmblock));
}

/* log replay */

void log_read(struct pmblock *block, struct pmblock log[logsize], unsigned i)
{
	cell_t *mem = log[i].data;
	cell_t *ram = block->data;
	cell_t savebits = mem[blockcells - 1];

	for (unsigned cell = 0, shift = 0x40; cell < blockcells - 1; cell++)
		ram[cell] = (mem[cell] & ~3) | (savebits >> (shift -= 2) & 3);
	ram[blockcells - 1] = 0;
}

bool log_valid(struct pmblock log[logsize], unsigned i)
{
	unsigned sum = 0;

	for (unsigned cell = 0; cell < blockcells; cell++)
		sum += log[i].data[cell] & 1;

	return !(sum % linecells);
}

bool log_less(struct pmblock log[logsize], unsigned i, unsigned j)
{
	return ((log[i].data[0] - log[j].data[0]) & 3) < 0;
}

unsigned successor(unsigned i)
{
	return (i + 1) & logmask;
}

void log_clear(struct pmblock log[logsize])
{
	for (int i = 0; i < logsize; i++) {
		memset(&log[i], 0, sizeof log[i]);
		memset(&log[i], -1, 1);
	}
}
