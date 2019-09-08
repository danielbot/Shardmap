#define VARTAG 0
#include "recops.c"
#undef VARTAG

struct bhbase : recinfo
{
	loc_t loc;
	bhbase(void *data, unsigned size, unsigned reclen) : recinfo{(u8 *)data, size, reclen}, loc{-1} {}
	#include "recops.inc"
};

struct bh : bhbase
{
	bh(void *data, unsigned size, unsigned reclen) : bhbase{data, size, reclen} {}
};

namespace varops {
	#define VARTAG 1
	#include "recops.c"
	#undef VARTAG

	struct bh : bhbase
	{
		bh(void *data, unsigned size, unsigned reclen) : bhbase{data, size, reclen} {}
		#include "recops.inc"
	};
}

