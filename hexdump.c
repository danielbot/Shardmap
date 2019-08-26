void hexdump(const void *data, unsigned size) // (c) 2009-2011 Daniel Phillips, GPL v2
{
	const unsigned char *p, *q = (unsigned char *)data;
	while (size) {
		unsigned w = 16, n = size < w ? size : w, pad = w - n;
		printf("%p:  ", q);
		for (p = q; p < q + n;)
			printf("%02x ", *p++);
		printf("%*.s  \"", pad*3, "");
		for (p = q; p < q + n;) {
			int c = *p++;
			printf("%c", c < ' ' || c > 126 ? '.' : c);
		}
		printf("\"\n");
		size -= n;
		q += w;
	}
}
