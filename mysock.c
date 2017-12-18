#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <luajit-2.0/lua.h>
#include <luajit-2.0/lauxlib.h>
#include <luajit-2.0/lualib.h>
#include <openssl/sha.h>

#define RET_DONE  0
#define RET_MORE  1
#define RET_AGAIN 2

#define PACK_HEAD 0x00
#define PACK_BODY 0x01

#define PACK_MAX_SIZE 0xFFFFFF

static inline uint32_t ToInt3(char* buf) {
	return buf[0] | buf[1] << 8 | buf[2] << 16;
}

typedef struct DbDriver {
	uint8_t  state;
	int16_t  seq;
	bool     more;
	int32_t  fd;
	uint32_t len;
} DbDriver_t, *DbDriver;

int DbDriver_new(lua_State* L) {
	DbDriver dd = lua_newuserdata(L, sizeof(DbDriver_t));
	dd->state = PACK_HEAD;
	dd->fd = -1;
	dd->len = 0;
	dd->seq = -1;
	dd->more = false;
	return 1;
}

int DbDriver_close(lua_State* L) {
	DbDriver dd = lua_touserdata(L, -1);
	if(dd->fd >= 0) {
		close(dd->fd);
	}
	return 0;
}

int DbDriver_connect(lua_State* L) {
	DbDriver dd = lua_touserdata(L, -3);
	const char* addr = lua_tostring(L, -2);
	short port = lua_tointeger(L, -1);

	printf("Addr: %s | Port: %d\n", addr, port);

	if(dd->fd == -1) {
		dd->fd = socket(AF_INET, SOCK_STREAM, 0);
		if(dd->fd == -1) {
			goto err;
		}
	}
	struct sockaddr_in sa = {
		.sin_family = AF_INET,
		.sin_port = htons(port),
		.sin_addr.s_addr = inet_addr(addr)
	};

	int ret = connect(dd->fd, (struct sockaddr*)&sa, sizeof(struct sockaddr_in));
	if(ret == -1) {
		goto err;
	}
	lua_pushboolean(L, true);
	return 1;
err:
	if(errno == EALREADY || errno == EINPROGRESS) {
		return lua_yield(L, 0);
	}
	luaL_error(L, strerror(errno));
}

int DbDriver_sendPacket(lua_State* L) {
	DbDriver dd = lua_touserdata(L, -2);
	size_t slen = 0;
	const char* data = lua_tolstring(L, -1, &slen);

	const char* start = data;
	while(slen) {
		size_t blkLen = (slen >= PACK_MAX_SIZE) ? PACK_MAX_SIZE : slen;

		/* construct packet head */
		char head[4] = {blkLen & 0xFF, (blkLen >> 8) & 0xFF, (blkLen >> 16) & 0xFF, ++dd->seq};
		/* send packet header */
		size_t len = 0;
		while(4 - len) {
			ssize_t ret = write(dd->fd, &head[len], 4 - len);
			if(ret < 0) {
				goto err;
			}
			len += ret;
		}

		/* send packet body */
		len = 0;
		while(blkLen - len) {
			ssize_t ret = write(dd->fd, &start[len], blkLen - len);
			if(ret < 0) {
				goto err;
			}
			len += ret;
		}

		slen -= blkLen;
		start += blkLen;
		++dd->seq;
	}
	lua_pushboolean(L, true);
	return 1;
err:
	if(errno == EAGAIN) {
		return lua_yield(L, 0);
	}
	luaL_error(L, strerror(errno));
}

int DbDriver_recvPacket(lua_State* L) {
	DbDriver dd = lua_touserdata(L, -1);
	char buf[4096];
	size_t len = 0;
	switch(dd->state) {
		case PACK_HEAD:
			while(4 - len) {
				ssize_t ret = read(dd->fd, &buf[len], 4 - len);
				if(ret <= 0) {
					goto err;
				}
				len += ret;
			}
			dd->len = buf[0] | buf[1] << 8 | buf[2] << 16;
			dd->seq = buf[3];
			dd->state = PACK_BODY;
			dd->more = dd->len == PACK_MAX_SIZE;
		case PACK_BODY:
			len = 0;
			while((dd->len - len) && (4096 - len)) {
				ssize_t ret = read(dd->fd, &buf[len], dd->len - len);
				if(ret <= 0) {
					goto err;
				}
				len += ret;
			}
			dd->len -= len;
			if(dd->len == 0) {
				dd->state = PACK_HEAD;
			}
	}
	/* has more data */
	lua_pushboolean(L, !(dd->len > 0 || dd->more));
	lua_pushlstring(L, buf, len);
	return 2;
err:
	lua_pushnil(L);
	if(errno == EAGAIN) {
		return lua_yield(L, 0);
	}
	luaL_error(L, strerror(errno));
}

int DbDriver_resetSeq(lua_State* L) {
	DbDriver dd = lua_touserdata(L, -1);
	dd->seq = -1;
	return 0;
}

int DbDriver_getToken(lua_State* L) {
	size_t l1, l2;
	const char* passwd = lua_tolstring(L, -2, &l1);
	const char* scramble = lua_tolstring(L, -1, &l2);

	char s1[20];
	char s3[20 + l2];
	SHA1(passwd, l1, s1);
	SHA1(s1, 20, s3 + l2);
	memcpy(s3, scramble, l2);
	SHA1(s3, 20 + l2, s3);

	for(uint8_t i = 0; i < 20; ++i) {
		s1[i] = s3[i] ^ s1[i];
	}

	lua_pushlstring(L, s1, 20);
	return 1;
}

static const struct luaL_Reg mysock[] = {
	{"new"    , DbDriver_new},
	{"close"  , DbDriver_close},
	{"connect", DbDriver_connect},
	{"send"   , DbDriver_sendPacket},
	{"recv"   , DbDriver_recvPacket},
	{"reset"  , DbDriver_resetSeq},
	{"getToken", DbDriver_getToken},
	{NULL     , NULL}
};

int luaopen_mysock(lua_State* L) {
	luaL_register(L, "mysock", mysock);
	return 1;
}
