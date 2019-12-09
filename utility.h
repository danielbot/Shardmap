uint64_t keyhash(const void *in, unsigned len);
int uform(char *buf, int len, unsigned long n, unsigned base);
const char *cprinz(const void *text, unsigned len);

static u64 power2(unsigned power) { return 1LL << power; }
static u64 bitmask(unsigned bits) { return power2(bits) - 1; }
static u64 align(u64 n, unsigned bits) { return n + (-n & bitmask(bits)); }
