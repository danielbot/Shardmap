enum {taglen = 0}; // optional one byte variable data length borrowed from key

#include "recops.c"

struct ri : recinfo
{
	ri(void *data, unsigned size, unsigned reclen) : recinfo{size, reclen, (u8 *)data} {}
	#include "recops.inc"
};

namespace varops {
	enum {taglen = 1};

	#include "recops.c"

	struct vri : ri
	{
		vri(void *data, unsigned size, unsigned reclen) : ri{data, size, reclen} {}
		#include "recops.inc"
	};
}

