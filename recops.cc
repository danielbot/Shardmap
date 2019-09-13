struct recinfo { const unsigned blocksize, reclen; u8 *data; loc_t loc; };
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

typedef void (rb_walk_fn)(void *context, u8 *key, unsigned keylen, u8 *data, unsigned reclen);

static u8 rb_hash(u16 ihash) { return ihash % 255; }

struct ribase : recinfo
{
	ribase(void *data, unsigned size, unsigned reclen);
	~ribase();

	virtual struct rb *irb() = 0;
	virtual struct rb *irbrec(rec_t **rec) = 0;
	virtual unsigned rb_gap(struct rb *rb) = 0;
public:
	virtual void init() = 0;
	virtual int big() = 0;
	virtual int more() = 0;
	virtual void dump() = 0;
	virtual void *key(unsigned which, unsigned *ret) = 0;
	virtual bool check() = 0;
	virtual rec_t *lookup(const void *key, u8 len, u16 lowhash) = 0;
	virtual rec_t *varlookup(const void *key, u8 len, u16 lowhash, u8 *varlen) = 0;
	virtual rec_t *create(const void *newkey, u8 newlen, u16 lowhash, const void *newrec) = 0;
	virtual rec_t *create(const void *newkey, u8 newlen, u16 lowhash, const void *newrec, u8 varlen) = 0;
	virtual int remove(const void *key, u8 len, u16 lowhash) = 0;
	virtual int walk(rb_walk_fn fn, void *context) = 0;
};

ribase::ribase(void *data, unsigned size, unsigned reclen) : recinfo{size, reclen, (u8 *)data} {}
ribase::~ribase() {}

struct ri : ribase
{
	enum {taglen = 0}; // optional one byte variable data borrowed from key
	ri(void *data, unsigned size, unsigned reclen) : ribase{(u8 *)data, size, reclen} {}
	ri(const ri &ri) : ribase{(u8 *)ri.data, ri.blocksize, ri.reclen} {} // copy constructor
	#include "recops.inc"
};

struct vri : ri
{
	enum {taglen = 1}; // record data can vary from reclen to reclen + 254
	vri(void *data, unsigned size, unsigned reclen) : ::ri{data, size, reclen} {}
	#include "recops.inc"
};
