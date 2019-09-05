#include "recops.c"

struct bh : recinfo
{
	bh (void *data, unsigned size, unsigned reclen) : recinfo{(u8 *)data, size, reclen} {}

	virtual int walk(rb_walk_fn fn, void *context)
	{
		return rb_walk(*this, fn, context);
	}

	virtual int big()
	{
		return rb_big(*this);
	}

	virtual int more()
	{
		return rb_more(*this);
	}

	virtual void dump()
	{
		rb_dump(*this);
	}

	virtual bool check()
	{
		return rb_check(*this);
	}

	virtual void *key(unsigned which, unsigned *len)
	{
		return rb_key(*this, which, len);
	}

	virtual rec_t *create(const void *key, u8 len, u16 lowhash, const void *data)
	{
		return rb_create(*this, key, len, lowhash, data, 0);
	}

	virtual rec_t *lookup(const void *key, u8 len, u16 lowhash)
	{
		return rb_lookup(*this, key, len, lowhash);
	}

	virtual int remove(const void *key, u8 len, u16 lowhash)
	{
		return rb_delete(*this, key, len, lowhash);
	}

	virtual void init()
	{
		rb_init(*this);
	}

//	virtual bool is_rb()
//	{
//		return ::is_rb(*this);
//	}
};
