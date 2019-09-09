enum {maxname = 255};

typedef u8 rec_t;
typedef void (rb_walk_fn)(void *context, u8 *key, unsigned keylen, u8 *data, unsigned reclen);

struct recinfo { u8 *data; const unsigned size, reclen; loc_t loc = -1; };

int rb_walk(struct recinfo *ri, rb_walk_fn fn, void *context);
int rb_big(struct recinfo *ri);
int rb_more(struct recinfo *ri);
void rb_dump(struct recinfo *ri);
bool rb_check(struct recinfo *ri);
void *rb_key(struct recinfo *ri, unsigned which, unsigned *len);
rec_t *rb_create(struct recinfo *ri, const void *newkey, u8 newlen, u16 lowhash, const void *data, u8 varlen);
rec_t *rb_lookup(struct recinfo *ri, const void *key, u8 len, u16 lowhash);
int rb_delete(struct recinfo *ri, const void *key, u8 len, u16 lowhash);
void rb_init(struct recinfo *ri);
//bool is_rb(struct recinfo *ri); // implement me, use me!
