#define USED __attribute__((__used__))

enum {max_errno = 4095}; // not really the right place for this, but...

USED static void *errwrap(long err) { /*asser(err < 0);*/ return (void *)err; }
USED static long errcode(const void *p) { return (long)p; }
USED static bool is_errcode(const void *p) { return (unsigned long)p >= -(unsigned long)max_errno; }

void hexdump(const void *data, unsigned size);
void errno_exit(unsigned exitcode);
void error_exit(unsigned exitcode, const char *reason, ...);
void bt(int fd);

#define BREAK asm("int3")

#if 1
#define assert(cond) do { if (!(cond)) error_exit(255, "Failed assert(" #cond ")"); } while (0)
#else
#define assert(cond) do {} while (0)
#endif

#define logline(fmt, ...) printf(fmt, ##__VA_ARGS__)
#define trace_off(...) do {} while (0)
#define trace_on(fmt, ...) do { logline("%s: " fmt "\n" , __func__, ##__VA_ARGS__); } while (0)
