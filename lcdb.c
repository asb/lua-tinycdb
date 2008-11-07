#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <lua.h>
#include <lauxlib.h>

#include "cdb.h"

#define LCDB_DB "cdb.db"
#define LCDB_MAKE "cdb.make"

static struct cdb *new_cdb(lua_State *L) {
  struct cdb *cdbp = (struct cdb*)lua_newuserdata(L, sizeof(struct cdb));
  luaL_getmetatable(L, LCDB_DB);
  lua_setmetatable(L, -2);
  return cdbp;
}

static struct cdb *check_cdb(lua_State *L, int n) {
  struct cdb *cdbp = (struct cdb*)luaL_checkudata(L, n, LCDB_DB);
  luaL_argcheck(L, cdbp->cdb_fd >= 0, n, "attempted to use a closed cdb");
  return cdbp;
}

static int push_errno(lua_State *L, int xerrno) {
  lua_pushnil(L);
  lua_pushstring(L, strerror(xerrno));
  return 2;
}

/* cdb.open(filename) */
static int lcdb_open(lua_State *L) {
  struct cdb *cdbp;
  const char *filename = luaL_checkstring(L, 1);
  int ret;

  int fd = open(filename, O_RDONLY);
  if (fd < 0)
    return push_errno(L, errno);

  cdbp = new_cdb(L);
  ret = cdb_init(cdbp, fd);
  if (ret < 0) {
    lua_pushnil(L);
    lua_pushfstring(L, LCDB_DB": file %s is not a valid database (or mmap failed)", filename);
    return 2;
  }
  return 1;
}

/* db:close() */
static int lcdbm_gc(lua_State *L) {
  struct cdb *cdbp = (struct cdb*)luaL_checkudata(L, 1, LCDB_DB);
  if (cdbp->cdb_fd >= 0) {
    close(cdbp->cdb_fd);
    cdb_free(cdbp);
    cdbp->cdb_fd = -1;
  }
  return 0;
}

/* db:__tostring() */
static int lcdbm_tostring(lua_State *L) {
  struct cdb *cdbp = (struct cdb*)luaL_checkudata(L, 1, LCDB_DB);
  if (cdbp->cdb_fd >= 0)
    lua_pushfstring(L, "<"LCDB_DB"> (%p)", cdbp);
  else
    lua_pushfstring(L, "<"LCDB_DB"> (closed)");
  return 1;
}

/* db:get(key) */
static int lcdbm_get(lua_State *L) {
  size_t klen;
  int ret;
  struct cdb *cdbp = check_cdb(L, 1);
  const char *key = luaL_checklstring(L, 2, &klen);

  ret = cdb_find(cdbp, key, klen);
  if (ret > 0) {
    lua_pushlstring(L, cdb_getdata(cdbp), cdb_datalen(cdbp));
    return 1;
  } else if (ret == 0) {
    lua_pushnil(L);
    return 1;
  } else {
    return luaL_error(L, LCDB_DB": error in find. Database corrupt?");
  }
}

/* db:find_all(key) */
static int lcdbm_find_all(lua_State *L) {
  size_t klen;
  int ret;
  int n = 1;
  struct cdb *cdbp = check_cdb(L, 1);
  const char *key = luaL_checklstring(L, 2, &klen);

  struct cdb_find cdbf;
  cdb_findinit(&cdbf, cdbp, key, klen);

  lua_newtable(L);
  while((ret = cdb_findnext(&cdbf))) {
    if (ret < 0) { /* error */
      return luaL_error(L, LCDB_DB": error in find_all. Database corrupt?");
    }

    lua_pushlstring(L, cdb_getdata(cdbp), cdb_datalen(cdbp));
    lua_rawseti(L, -2, n);
    n++;
  }
  return 1;
}
  
static int lcdbm_iternext(lua_State *L) {
  struct cdb *cdbp = (struct cdb*)lua_touserdata(L, lua_upvalueindex(1));
  unsigned pos = lua_tointeger(L, lua_upvalueindex(2));

  int ret = cdb_seqnext(&pos, cdbp);
  lua_pushinteger(L, pos);
  lua_replace(L, lua_upvalueindex(2));
  if (ret > 0) {
    lua_pushlstring(L, cdb_getkey(cdbp), cdb_keylen(cdbp));
    lua_pushlstring(L, cdb_getdata(cdbp), cdb_datalen(cdbp));
    return 2;
  } else if (ret == 0) { /* finished */
    lua_pushnil(L);
    return 1;
  } else { /* error */
    return luaL_error(L, LCDB_DB": error in iterator. Database corrupt?");
  }
}

/* for k, v in db:pairs() do ... end */
static int lcdbm_pairs(lua_State *L) {
  struct cdb *cdbp = check_cdb(L, 1);

  unsigned pos;
  cdb_seqinit(&pos, cdbp);
  lua_pushinteger(L, pos);
  lua_pushcclosure(L, lcdbm_iternext, 2);
  return 1;
}

static struct cdb_make *new_cdb_make(lua_State *L) {
  struct cdb_make *cdbmp = (struct cdb_make*)lua_newuserdata(L, sizeof(struct cdb_make));
  luaL_getmetatable(L, LCDB_MAKE);
  lua_setmetatable(L, -2);
  lua_newtable(L);
  lua_setfenv(L, 1);
  return cdbmp;
}

static struct cdb_make *check_cdb_make(lua_State *L, int n) {
  struct cdb_make *cdbmp = luaL_checkudata(L, n, LCDB_MAKE);
  luaL_argcheck(L, cdbmp->cdb_fd >= 0, n, "attempted to use a closed cdb_make");
  return cdbmp;
}

/* cdb.make(destination, temporary) */
static int lcdb_make(lua_State *L) {
  int fd;
  int ret;
  struct cdb_make *cdbmp;
  const char *dest = luaL_checkstring(L, 1);
  const char *tmpname = luaL_checkstring(L, 2);

  fd = open(tmpname, O_RDWR|O_CREAT|O_EXCL, 0666);
  if (fd < 0)
    return push_errno(L, errno);

  cdbmp = new_cdb_make(L);
  ret = cdb_make_start(cdbmp, fd);

  /* store destination and tmpname in userdata environment */
  lua_getfenv(L, -1);
  lua_pushstring(L, dest);
  lua_setfield(L, -2, "dest");
  lua_pushstring(L, tmpname);
  lua_setfield(L, -2, "tmpname");
  lua_pop(L, 1); /* pop the environment */

  if (ret < 0)
    return push_errno(L, errno);
  return 1;
}

static int lcdbmakem_gc(lua_State *L) {
  struct cdb_make *cdbmp = luaL_checkudata(L, 1, LCDB_MAKE);

  if (cdbmp->cdb_fd >= 0) {
    close(cdbmp->cdb_fd);
    cdb_make_free(cdbmp);
    cdbmp->cdb_fd = -1;
  }
  return 0;
}

/* maker:__tostring() */
static int lcdbmakem_tostring(lua_State *L) {
  struct cdb_make *cdbmp = luaL_checkudata(L, 1, LCDB_MAKE);

  if (cdbmp->cdb_fd >= 0)
    lua_pushfstring(L, "<"LCDB_MAKE"> (%p)", cdbmp);
  else
    lua_pushfstring(L, "<"LCDB_MAKE"> (closed)");
  return 1;
}

/* maker:add(key, value, [mode]) */
static int lcdbmakem_add(lua_State *L) {
  static const char *const opts[] = { "add", "replace", "replace0", "insert", NULL };
  size_t klen, vlen;
  struct cdb_make *cdbmp = check_cdb_make(L, 1);
  const char *key = luaL_checklstring(L, 2, &klen);
  const char *value = luaL_checklstring(L, 3, &vlen);
  /* by default, add unconditionally */
  int mode = luaL_checkoption(L, 4, "add", opts);

  int ret = cdb_make_put(cdbmp, key, klen, value, vlen, mode);
  if (ret < 0)
    return luaL_error(L, strerror(errno));
  return 0;
}

/* maker:finish() */
static int lcdbmakem_finish(lua_State *L) {
  struct cdb_make *cdbmp = check_cdb_make(L, 1);
  /* retrieve destination, current filename */
  lua_getfenv(L, -1);
  lua_getfield(L, -1, "dest");
  const char *dest = lua_tostring(L, -1);
  lua_getfield(L, -2, "tmpname");
  const char *tmpname = lua_tostring(L, -1);
  lua_pop(L, 3);

  if (cdb_make_finish(cdbmp) < 0 || fsync(cdb_fileno(cdbmp)) < 0 || 
      close(cdb_fileno(cdbmp)) < 0 || rename(tmpname, dest) < 0) {
    cdb_make_free(cdbmp); /* in case cdb_make_finish failed before freeing */
    cdbmp->cdb_fd = -1;
    return luaL_error(L, strerror(errno));
  }

  cdbmp->cdb_fd = -1;
  lua_pushboolean(L, 1);
  return 1;
}

static const struct luaL_Reg lcdb_f [] = {
  {"open", lcdb_open},
  {"make", lcdb_make},
  {NULL, NULL}
};

static const struct luaL_Reg lcdb_m [] = {
  {"__gc", lcdbm_gc},
  {"close", lcdbm_gc},
  {"__tostring", lcdbm_tostring},
  {"find_all", lcdbm_find_all},
  {"get", lcdbm_get},
  {"pairs", lcdbm_pairs},
  {"iter", lcdbm_pairs},
  {NULL, NULL}
};

static const struct luaL_Reg lcdbmake_m [] = {
  {"__gc", lcdbmakem_gc},
  {"__tostring", lcdbmakem_tostring},
  {"add", lcdbmakem_add},
  {"finish", lcdbmakem_finish},
  {NULL, NULL}
};

int luaopen_cdb(lua_State *L) {
  luaL_newmetatable(L, LCDB_DB);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_register(L, NULL, lcdb_m);

  luaL_newmetatable(L, LCDB_MAKE);
  lua_pushvalue(L, -1);
  lua_setfield(L, -2, "__index");
  luaL_register(L, NULL, lcdbmake_m);

  lua_newtable(L);
  luaL_register(L, NULL, lcdb_f);

  return 1;
}
