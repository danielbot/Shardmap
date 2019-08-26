typedef u32 loc_t;

enum {bigmap_maxlevels = 10};

struct datamap
{
	u8 *data;
	loc_t loc;
};

struct bigmap
{
	unsigned blocksize, blockbits, levels;
	loc_t blocks, maxblocks;
	struct level {
		struct datamap map;
		u16 start, at, wrap, big;
	} path[bigmap_maxlevels];
	bool partial_path;
	u8 big;
	u16 reclen; // this is only here because some ext_bigmap functions need it. Fix!!!
	u8 *rbspace; // this is only here because we have not properly abstracted the block mapping yet!!!
};

/* Exports */

void bigmap_open(struct bigmap *map);
void bigmap_close(struct bigmap *map);
int bigmap_try(struct bigmap *map, unsigned len, unsigned big); // maybe bigmap should do ext_bigmap_big itself?
int bigmap_free(struct bigmap *map, loc_t loc, unsigned big);
size_t bigmap_check(struct bigmap *map);
void bigmap_dump(struct bigmap *map);
void bigmap_load(struct bigmap *map, loc_t loc); // rationalize me... inits path level[0]
void add_new_rec_block(struct bigmap *map);
bool is_maploc(loc_t loc, unsigned blockbits);

/* Imports */

u8 *ext_bigmap_mem(struct bigmap *map, loc_t loc);
void ext_bigmap_map(struct bigmap *map, unsigned level, loc_t loc);
void ext_bigmap_unmap(struct bigmap *map, struct datamap *dm);
unsigned ext_bigmap_big(struct bigmap *map, struct datamap *dm);
