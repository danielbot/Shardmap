struct tabent { u8 hash; u8 len; };

struct rb // record block format
{
	u16 size; /* blocksize */
	u16 used; /* entry headers and key text stored compactly at top of block */
	u16 free; /* total key text space in free entries within entry region */
	u16 count; /* total entries including free entries */
	u16 holes; /* number of free entries created by delete */
	char magic[2];
	struct tabent table[];
};

enum {tabent_size = sizeof(struct tabent), header_size = sizeof(struct rb)};
enum {cleanup = 0, cleaned = 0xee, deleted = 0xdd, holecode = 0xff, maxname = 255};

static u8 rb_hash(u16 ihash) { return ihash % 255; }

struct recops {
	/* Exports */
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

enum {taglen = 0}; // optional one byte variable data borrowed from key
