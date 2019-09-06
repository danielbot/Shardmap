namespace recops {
#define VARTAG 0
#include "recops.c"
#undef VARTAG
}

namespace varops {
#define VARTAG 1
#include "recops.c"
#undef VARTAG
}

using namespace recops;

struct bh : recinfo
{
	bh(void *data, unsigned size, unsigned reclen) : recinfo{(u8 *)data, size, reclen} {}

	#include "recops.inc"
};

using namespace varops;

struct vh : bh
{
	vh(void *data, unsigned size, unsigned reclen) : bh(data, size, reclen) {}

	#include "recops.inc"
};
