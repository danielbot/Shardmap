enum {maxname = 255};
enum {cleanup = 0, cleaned = 0xee, deleted = 0xdd, holecode = 0xff};

struct tabent { u8 hash; u8 len; };

struct rb
{
	u16 size; /* blocksize */
	u16 used; /* entry headers and key text stored compactly at top of block */
	u16 free; /* total key text space in free entries within entry region */
	u16 count; /* total entries including free entries */
	u16 holes; /* number of free entries created by delete */
	char magic[2];
	struct tabent table[];
};

struct recinfo { const unsigned blocksize, reclen; u8 *data; loc_t loc; };

typedef u8 rec_t;

typedef void (rb_walk_fn)(void *context, u8 *key, unsigned keylen, u8 *data, unsigned reclen);

static u8 rb_hash(u16 ihash) { return ihash % 255; }

enum {taglen = 0}; // optional one byte variable data length borrowed from key

struct ri : recinfo
{
	ri(void *data, unsigned size, unsigned reclen) : recinfo{size, reclen, (u8 *)data} {}
	#include "recops.inc"
};

namespace varops {
	enum {taglen = 1};

	struct vri : ri
	{
		vri(void *data, unsigned size, unsigned reclen) : ri{data, size, reclen} {}
		#include "recops.inc"
	};
}
