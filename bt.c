/*
 * Backtrace using libbacktrace
 * (c) 2017 Daniel Phillips
 * Plain C except for C++ demangle
 */

#if 1
//#include <cxxabi.h> // demangle
#include <string.h>
#include <dlfcn.h>
#include <backtrace.h>
#include <unistd.h>
#include <dlfcn.h>

int uform(char *buf, int len, unsigned long n, unsigned base);

static const char *demangle(const char *name)
{
#if 0
	if (name) {
		int ret = 0;
		const char *demangled = abi::__cxa_demangle(name, NULL, NULL, &ret);
		if (!ret)
			name = demangled;
	}
#endif
	return name;
}

struct btwork
{
	unsigned pos;
	unsigned max;
	int fd, err;
	struct backtrace_state *state;
	char buf[];
};

static void bt_emit(struct btwork *work, const char *text, unsigned len)
{
	if (len > work->max - work->pos)
		len = work->max - work->pos;
	memmove(work->buf + work->pos, text, len);
	work->pos += len;
}

static void bt_str(struct btwork *work, const char *str)
{
	if (!str)
		str = "(nul)";
	bt_emit(work, str, strlen(str));
}

static void bt_uform(struct btwork *work, unsigned long n, unsigned char base)
{
	work->pos += uform(work->buf + work->pos, work->max - work->pos, n, base);
}

static void bt_xform(struct btwork *work, unsigned long n)
{
	bt_emit(work, "0x", 2);
	bt_uform(work, n, 0x10);
}

static void bt_char(struct btwork *work, char c)
{
	bt_emit(work, &c, 1);
}

#if 0
static char bt_last(struct btwork *work)
{
	return !work->pos || work->pos == work->max ? -1 : work->buf[work->pos - 1];
}
#endif

static void bt_need(struct btwork *work, unsigned need)
{
	if (work->max - work->pos < need) {
		if (need > work->max)
			need = work->max;
		work->pos = work->max - need;
	}
}

static void bt_eol(struct btwork *work)
{
	bt_need(work, 1);
	bt_char(work, '\n');
}

static void bt_flush(struct btwork *work)
{
	if (work->pos) {
		write(work->fd, work->buf, work->pos);
		work->pos = 0;
	}
}

static void bt_line(struct btwork *work)
{
	bt_eol(work);
	bt_flush(work);
}

static void bt_error(void *data, const char *msg, int err)
{
	struct btwork *work = (struct btwork *)data;
	if (work->pos)
		bt_line(work);
	bt_emit(work, msg, strlen(msg));
	bt_emit(work, " (", 2);
	if (err < 1) {
		bt_char(work, '-');
		err = -err;
	}
	bt_uform(work, err, 0x10);
	bt_char(work, ')');
	bt_line(work);
	work->err = err;
	return;
}

static int bt_full(void *data, uintptr_t pc, const char *filename, int lineno, const char *funcname)
{
	if (1 && !funcname && !filename)
		return 0;

	if (1) {
		struct btwork *work = (struct btwork *)data;
		if (1) {
			bt_char(work, '[');
			bt_uform(work, getpid(), 10);
			bt_char(work, ':');
			bt_xform(work, pc);
			bt_str(work, "] ");
		}
		bt_str(work, filename);
		bt_char(work, ':');
		bt_uform(work, lineno, 10);
		bt_char(work, ' ');
		bt_str(work, demangle(funcname));
		bt_line(work);
	} else
		printf("[0x%lx] %s %s:%i\n", pc, demangle(funcname), filename, lineno);
	return 0;
}

static void bt_syminfo(void *data, uintptr_t pc, const char *symname, uintptr_t symval, uintptr_t symsize)
{
	struct btwork *work = (struct btwork *)data;
	if (1) {
		bt_char(work, '[');
		bt_xform(work, pc);
		bt_str(work, "] ");
		bt_xform(work, symval);
		bt_char(work, '/');
		bt_uform(work, symsize, 10);
		bt_char(work, ' ');
		bt_str(work, demangle(symname));
		bt_char(work, '+');
		bt_xform(work, pc - symval);
		bt_line(work);
	} else {
		printf("[0x%lx] 0x%lx/%lu %s+%lu\n", pc, symval, symsize, demangle(symname), pc - symval);
		Dl_info info;
		if (!dladdr((void *)pc, &info)) {
			fprintf(stderr, "%s\n", dlerror());
			return;
	}
		if (1)
			printf("%s %s %p %p\n", info.dli_fname, demangle(info.dli_sname), info.dli_fbase, info.dli_saddr);
    }
}

static int simple_callback(void *data, uintptr_t pc)
{
	struct btwork *work = (struct btwork *)data;
	if (pc == -1UL) return 0;
	if (1) backtrace_pcinfo(work->state, pc, bt_full, bt_error, data);
	if (0) backtrace_syminfo(work->state, pc, bt_syminfo, bt_error, data);
	return 0;
}

void bt(int fd)
{
	char data[256];
	struct btwork *work = (struct btwork *)data;
	*(work) = (struct btwork){0, sizeof data - sizeof(struct btwork), fd};
	work->state = backtrace_create_state(0 ? "/home/daniel/dlm/dlm" : NULL, 0, bt_error, work);

	if (fd == 1)
		fflush(stdout);
	backtrace_simple(work->state, 1, simple_callback, bt_error, work);

	if (work->err) {
		if (work->pos)
		    bt_line(work);
		bt_str(work, "backtrace failed\n");
		bt_flush(work);
	}
}

#else
#include <execinfo.h>

extern "C" void bt() // libc backtrace (sucks)
{
	void *buffer[5000];
	int nptrs = backtrace(buffer, sizeof buffer);
	backtrace_symbols_fd(buffer, nptrs, 1);
}
#endif

//--*(char *)0; // generate segfault
