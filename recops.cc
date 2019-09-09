#define VARTAG 0
#include "recops.c"
#undef VARTAG

struct ri : recinfo
{
	ri(void *data, unsigned size, unsigned reclen) : recinfo{size, reclen, (u8 *)data} {}
	#include "recops.inc"
};

namespace varops {
	#define VARTAG 1
	#include "recops.c"
	#undef VARTAG

	struct vri : ri
	{
		vri(void *data, unsigned size, unsigned reclen) : ri{data, size, reclen} {}
		#include "recops.inc"
	};
}

