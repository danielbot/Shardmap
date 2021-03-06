/*
 * Shardmap fast lightweight key value store
 * (c) 2012 - 2019, Daniel Phillips
 * License: GPL v3
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include "debug.h"

#include "hexdump.c"
#include "bt.c"

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
#ifdef DEBUG
	BREAK;
#endif
	exit(exitcode);
}

#include "siphash.c"

uint64_t keyhash(const unsigned char *in, unsigned len)
{
	const u8 seed[16] = {
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0x0f,
		0x0f, 0xed, 0xcb, 0xa9, 0x87, 0x65, 0x43, 0x21};
	return siphash(in, len, seed);
}

int uform(char *buf, int len, unsigned long n, unsigned base) // (c) 2019 Daniel Phillips, GPL v2
{
    const char digit[] = {"0123456789abcdef"};
    if (len) {
        char *p = buf + len;
        do *--p = digit[n % base]; while (p > buf && (n /= base));
        memmove(buf, p, len -= p - buf);
    }
    return len;
}

const char *cprinz(const void *text, unsigned len) // sanitize string unsafely
{
	static char unsafe[256];
	char *p = unsafe, *q = (char *)text;
	if (len > 255)
		len = 255;
	for (; len--; q++)
		*p++ = ((unsigned)*q - ' ') < (255 - ' ') ? *q : '.';
	*p = 0;
	assert(strlen(unsafe) < 256);
	return (const char *)unsafe;
}
