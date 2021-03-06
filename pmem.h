#include <x86intrin.h>

enum {noflush = 1, use_clwb = 0, use_intrinsics = 1, streaming = 1, verbose = 0};
enum {microlog_size = logsize * sizeof (struct pmblock)};

static void clflushopt(volatile void *p)
{
	asm volatile("clflushopt %P0" : "+m" (*(volatile char *)p));
}

static void clwb(volatile void *p)
{
	if (noflush)
		return;
	if (verbose)
		printf("clwb %p\n", p);
	if (use_clwb) {
		asm volatile("clwb (%[pax])" // originally from kernel (gpl)
			: [p] "+m" (*(volatile char *)p)
			: [pax] "a" ((volatile char *)p));
	} else
		clflushopt(p);
}

static void sfence(void)
{
	if (noflush)
		return;
	if (verbose)
		printf("sfence\n");
	if (use_intrinsics)
		_mm_sfence();
	else
		asm volatile("sfence" ::: "memory");
}

static void ntstore64(cell_t *to, cell_t value)
{
	_mm_stream_si64((long long int*)to, value);
}

void pmwrite(void *to, void *from, unsigned len);
void log_clear(struct pmblock log[logsize]);
void log_commit(struct pmblock log[logsize], void *data, unsigned len, unsigned *pgen);
void log_read(struct pmblock *block, struct pmblock log[logsize], unsigned i);
