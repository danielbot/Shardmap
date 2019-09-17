struct rb *irb() override
{
	struct rb *rb = (struct rb *)data;
	assert(!memcmp(rb->magic, "RB", 2));
	//assert(reclen == rb->reclen); // might add this field for redundancy
	return rb;
}

struct rb *irbrec(rec_t **rec) override
{
	struct rb *rb = irb();
	*rec = (rec_t *)(data + rb->size);
	return rb;
}

unsigned rb_gap(struct rb *rb) override
{
	return (u8 *)rb + rb->size - rb->used - (u8 *)(rb->table + rb->count);
}

/* Exports */

void init() override
{
	struct rb *rb = (struct rb *)data; // avoid assert by not using irb
	*rb = (struct rb){.size = (typeof rb->size)blocksize};
	memcpy(rb->magic, "RB", 2); // because c++ char array init is braindamaged
}

int big() override
{
	struct rb *rb = irb();
	unsigned overhead = reclen + tabent_size;
	unsigned gap = rb_gap(rb);
	unsigned big = rb->holes ? gap + rb->free : (gap > overhead ? gap - overhead : 0);
	return big > maxname ? maxname : big;
}

int more() override
{
	struct rb *rb = irb();
	return rb_gap(rb) + rb->free;
}

void dump() override
{
	rec_t *rec;
	struct rb *rb = irbrec(&rec);

	if (1)
		printf("%u entries: ", rb->count);
	char sep = 1 ? ' ' : '\n';
	for (unsigned i = 0; i < rb->count; i++) {
		unsigned keylen = rb->table[i].len;
		rec -= reclen + keylen;
		if (0)
			printf("%u\n", keylen);
		if (rb->table[i].hash == holecode)
			printf("(%u)%c", keylen, sep);
		else
			printf("'%s' %x.%u:%u%c", cprinz(rec + reclen, keylen), rb->table[i].hash, keylen, *(u32 *)rec, sep);
	}
	printf("gap %i free %i holes %i\n", rb_gap(rb), rb->free, rb->holes);
}

/* Look up entry by index for testing, later use for seekdir */
void *key(unsigned which, unsigned *ret)  override
{
	rec_t *rec;
	struct rb *rb = irbrec(&rec);
	if (which >= rb->count) {
		*ret = 0;
		return NULL;
	}
	for (unsigned i = 0; i < which; i++)
		rec = rec - (reclen + rb->table[i].len);
	rec -= reclen + (*ret = rb->table[which].len);
	return rb->table[which].hash == holecode ? NULL : (rec + reclen);
}

bool check() override
{
	rec_t *rec;
	struct rb *rb = irbrec(&rec);
	unsigned scan_entry_count = 0, scan_hole_count = 0, scan_hole_space = 0, scan_entry_space = 0;
	unsigned count = rb->count;
	unsigned max_entries = (rb->size - header_size) / (reclen + tabent_size + 1);
	unsigned errs = 0;

	if (count > max_entries && ++errs) {
		printf("too many entries (%u)\n", count);
		count = max_entries;
	}

	void *table_top = rb->table + count;

	for (unsigned i = 0; i < count; i++) {
		unsigned keylen = rb->table[i].len;
		rec -= reclen + keylen;

		if ((void *)rec < table_top && ++errs) {
			printf("entries overlap table\n");
			break;
		}

		if (rb->table[i].hash == holecode) {
			scan_hole_space += keylen;
			scan_hole_count++;
		} else {
			scan_entry_space += keylen;
			scan_entry_count++;
		}
	}

	if (rb->holes != scan_hole_count && ++errs)
		printf("holes count (%u) wrong - found %u holes\n", rb->holes, scan_hole_count);

	if (rb->free > rb->used - reclen * count && ++errs)
		printf("free records (%u) more than total records (%u)\n",
			rb->free,
			rb->used - reclen * count);

	if (reclen * count + scan_entry_space != rb->used - rb->free && ++errs)
		printf("wrong entry space count (%u): %u entries with %u bytes, %u bytes in holes (%u)\n",
			rb->used,
			scan_entry_count,
			reclen * count + scan_entry_space,
			scan_hole_space,
			reclen * count + scan_entry_space + scan_hole_space);

	if (scan_hole_space != rb->free && ++errs)
		printf("wrong hole space count (%u): %u holes found with %u unused bytes\n",
			rb->free,
			scan_hole_count,
			scan_hole_space);

	return errs;
}

rec_t *lookup(const void *key, u8 len, u16 lowhash) override
{
	rec_t *rec;
	struct rb *rb = irbrec(&rec);
	unsigned hash = rb_hash(lowhash);
	trace("'%s' hash %i tag %i", cprinz(key, len), hash, taglen);
	assert(hash != holecode);

	for (unsigned i = 0; i < rb->count; i++) {
		unsigned keylen = rb->table[i].len;
		rec = rec - (reclen + keylen);
		unsigned varlen = taglen ? rec[0] : 0;
		trace("hash %x %x len %u", hash, rb->table[i].hash, len);
		if (rb->table[i].hash == hash && keylen == len) {
			if (!memcmp(key, rec + reclen + varlen, keylen - varlen))
				return rec + taglen;
		}
	}
	return NULL;
}

rec_t *varlookup(const void *key, u8 len, u16 lowhash, u8 *varlen) override
{
	rec_t *rec = lookup(key, len, lowhash);
	if (rec)
		*varlen = ((u8 *)rec)[-1];
	return rec;
}

rec_t *create(const void *newkey, u8 newlen, u16 lowhash, const void *newrec) override
{
	return create(newkey, newlen, lowhash, newrec, 0);
}

rec_t *create(const void *newkey, u8 newlen, u16 lowhash, const void *newrec, u8 varlen) override
{
	struct rb *rb = irb();
	unsigned gap = rb_gap(rb), last = rb->count - 1, pos = last;
	rec_t *rec;
	trace("'%s' hash %i tag %i gap %u free %u", cprinz(newkey, newlen), rb_hash(lowhash), taglen, gap, rb->free);

	int need, holespace;
	unsigned keylen;
	unsigned use_entry;
	rec_t *last_re;

	if (gap >= reclen + newlen + tabent_size) {
		trace("fast path create");
		rb->used += reclen + newlen;
		rec = (u8 *)data + rb->size - rb->used;
		pos = rb->count++;
		goto create;
	}

	/*
	 * Here we have already mostly computed the largest key/value that could fit
	 * in the block and caller probably wants to know that so the space could be
	 * used in future for some smaller record. So consider returning that as a
	 * a positive number, and use -ENOSPC only for exactly zero. So any nonzero
	 * return means the create failed without any changes. Negative returns need
	 * to be tested to determine whether it means exactly full or something more
	 * problematic. Reconsider all this in the light of errcode pointer wrapper.
	 */
	if (!rb->holes || gap + rb->free < newlen)
		return (rec_t *)errwrap(-ENOSPC);

	/* walk backward in dict until enough hole space found */
	/*rec_t **/ last_re = (u8 *)data + rb->size - rb->used;
	/*int*/ need = newlen - gap;
	/*int*/ holespace = 0;
	rec = last_re;
	/*unsigned keylen;*/

	do {
		keylen = rb->table[pos].len;
		trace_off(rec, reclen + keylen);
		trace("-- holespace = %i need = %i pos = %i", holespace, need, pos);
		if (rb->table[pos].hash == holecode) {
			holespace += keylen;
			if (holespace >= need)
				break;
		} else if (pos == 0)
			return (rec_t *)errwrap(-EIO); // reusable hole not found due to corruption or bug
		rec = rec + reclen + rb->table[pos--].len;
	} while (1);

	/*unsigned*/ use_entry = pos;
	holespace -= keylen; // create new entry here using this header
	need = newlen - keylen;
	trace("==> create new entry at %u newlen %u keylen %u", pos, newlen, keylen);
	/*
	 * Found a free entry, there are now three cases:
	 *  1) need < 0: too small, move down
	 *  2) need > 0: too large, move up
	 *  3) need = 0: same size, no action
	 */
	if (need > 0) {
		/* Case 1: Step backward through records, moving records down and shrinking holes */
		int movedown = need - holespace;
		if (movedown < 0)
			movedown = 0;
		rb->used += movedown; /* decrease gap */
		trace("movedown = %i pos = %i need = %i gap = %i", movedown, pos, need, rb_gap(rb));
		rec = last_re;
		pos = last;

		do {
			keylen = rb->table[pos].len;
			trace_off(rec, reclen + keylen);
			trace("-- pos = %i movedown = %i need = %i", pos, movedown, need);
			if (pos == use_entry)
				break;
			if (rb->table[pos].hash == holecode) {
				rec_t *newent = rec - movedown;
				int newlen = keylen - need;
				if (newlen < 0)
					newlen = 0;
				unsigned shrink = keylen - newlen;
				trace("shrink hole from %i to %i", keylen, newlen);
				if (cleanup)
					memset(newent, cleaned, newlen);
				assert(newlen <= maxname);
				rb->table[pos].len = newlen;
				rb->free -= shrink;
				movedown += shrink;
				need -= shrink;
			} else
				memmove(rec - movedown, rec, reclen + keylen);
			rec = rec + reclen + keylen;
			pos--;
		} while (1);
		assert(newlen - keylen == movedown);
		rec = rec - movedown;
		goto reuse;
	}

	if (need < 0) {
		/* Case 2: Step forward through records, moving records up, expand the first hole found  */
		int moveup = -need;
		need = 0;
		rec_t *use_re = rec + moveup;
		if (use_entry < last) do {
			keylen = rb->table[++pos].len;
			rec = rec - (reclen + keylen);
			trace("++ holespace = %i len = %u moveup = %i pos = %i last = %i", holespace, keylen, moveup, pos, last);
			trace_off(rec, reclen + keylen);
			if (rb->table[pos].hash == holecode) {
				if (cleanup)
					memset(rec + reclen + keylen, cleaned, moveup);
				assert(rb->table[pos].len + moveup <= maxname);
				rb->table[pos].len += moveup;
				rb->free += moveup;
				moveup = 0;
				break;
			}
			memmove(rec + moveup, rec, reclen + keylen);
			if (pos == last)
				break;
		} while (1);

		if (pos == last) {
			if (cleanup)
				memset((u8 *)data + rb->size - rb->used, 0, moveup);
			rb->used -= moveup; /* increase gap */
		}

		trace("gap = %u", rb_gap(rb));
		trace("old len = %u", rb->table[use_entry].len);
		rec = use_re;
		pos = use_entry;
		keylen = rb->table[use_entry].len;
		goto reuse;
	}

	trace("exact fit!");
reuse:
	rb->free -= keylen;
	rb->holes--;
create:
	rb->table[pos] = (struct tabent){rb_hash(lowhash), newlen};
	memcpy(rec + taglen, newrec, reclen - taglen + varlen);
	memcpy(rec + reclen + varlen, newkey, newlen - varlen);
	if (taglen)
		rec[0] = varlen;
	return rec;
}

int remove(const void *key, u8 len, u16 lowhash) override
{
	rec_t *rec;
	struct rb *rb = irbrec(&rec);
	unsigned hash = rb_hash(lowhash);
	trace("'%s' hash %i tag %i", cprinz(key, len), hash, taglen);

	for (unsigned i = 0; i < rb->count; i++) {
		unsigned keylen = rb->table[i].len;
		rec = rec - (reclen + keylen);
		unsigned varlen = taglen ? rec[0] : 0;
		if (rb->table[i].hash == hash && keylen == len) {
			if (!memcmp(key, rec + reclen + varlen, keylen - varlen)) {
				if (cleanup)
					memset(rec + reclen, cleaned, keylen);
				rb->table[i].hash = holecode;
				rb->free += keylen;
				rb->holes++;
				if (i == rb->count - 1) {
					trace("--- trim %i ---", i); // need unit test to verify all trimmed
					do {
						keylen = rb->table[rb->count = i].len;
						rb->free -= keylen;
						rb->used -= reclen + keylen;
						rb->holes--;
						if (cleanup) {
							memset(rec, 0, reclen + keylen);
							rb->table[i] = (struct tabent){};
						}
						rec = rec + (reclen + keylen);
					} while (i-- && rb->table[i].hash == holecode);
				}
				return 0;
			}
		}
	}
	return -ENOENT;
}

int walk(rb_walk_fn fn, void *context) override
{
	rec_t *rec;
	struct rb *rb = irbrec(&rec);

	for (unsigned i = 0; i < rb->count; i++) {
		unsigned keylen = rb->table[i].len;
		rec -= reclen + keylen;
		unsigned varlen = taglen ? rec[0] : 0;
		if (rb->table[i].hash != holecode)
			fn(context, rec + reclen + varlen, keylen - varlen, rec + taglen, reclen - taglen + varlen);
	}
	return 0;
}
