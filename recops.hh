struct rb : recinfo
{
	rb (void *data, unsigned size, unsigned reclen);
	virtual int walk(rb_walk_fn fn, void *context);
	virtual int big();
	virtual int more();
	virtual void dump();
	virtual bool check();
	virtual void *key(unsigned which, unsigned *len);
	virtual rec_t *create(const void *key, u8 len, u16 lowhash, const void *data);
	virtual rec_t *lookup(const void *key, u8 len, u16 lowhash);
	virtual int remove(const void *key, u8 len, u16 lowhash);
	virtual void init();
//	virtual bool is_rb();
};
