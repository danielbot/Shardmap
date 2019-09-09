#define VARTAG 0
#include "recops.c"
#undef VARTAG

struct bh : recinfo
{
	loc_t loc;
	bh(void *data, unsigned size, unsigned reclen) : recinfo{(u8 *)data, size, reclen}, loc{-1} {}
//	bh(const bh &bh); // copy constructor
	#include "recops.inc"
};

namespace varops {
	#define VARTAG 1
	#include "recops.c"
	#undef VARTAG

	struct vh : bh
	{
		vh(void *data, unsigned size, unsigned reclen) : bh{data, size, reclen} {}
		#include "recops.inc"
	};
}

