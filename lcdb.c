#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <lua.h>
#include <lauxlib.h>

#include "cdb.h"

#define LCDB_DB "cdb.db"
#define LCDB_MAKE "cdb.make"

static struct cdb *push_cdb(lua_State *L)
{
  struct cdb *cdbp = (struct cdb*)lua_newuserdata(L, sizeof(struct cdb));
  luaL_getmetatable(L, LCDB_DB);
  lua_setmetatable(L, -2);
  return cdbp;
}

static struct cdb *check_cdb(lua_State *L, int n)
{
  struct cdb *cdbp = (struct cdb*)luaL_checkudata(L, n, LCDB_DB);
  if (cdbp->cdb_fd < 0)
    luaL_error(L, "attempted to use a closed cdb");
  return cdbp;
}

static int push_errno(lua_State *L, int xerrno)
{
  lua_pushnil(L);
  lua_pushstring(L, strerror(xerrno));
  return 2;
}

static int lcdb_open(lua_State *L)
{
  struct cdb *cdbp;
  const char *filename = luaL_checkstring(L, 1);

  int fd = open(filename, O_RDONLY);
  if (fd < 0)
    return push_errno(L, errno);

  cdbp = push_cdb(L);
  cdb_init(cdbp, fd);
  return 1;
}

static int lcdbm_gc(lua_State *L)
{
  struct cdb *cdbp = (struct cdb*)luaL_checkudata(L, 1, LCDB_DB);
  if (cdbp->cdb_fd >= 0)
  {
    int fd = cdbp->cdb_fd;
    cdb_free(cdbp);
    close(fd);
    cdbp->cdb_fd = -1;
  }
  return 0;
}

static int lcdbm_tostring(lua_State *L)
{
  struct cdb *cdbp = (struct cdb*)luaL_checkudata(L, 1, LCDB_DB);
  if (cdbp->cdb_fd >= 0)
    lua_pushfstring(L, "<"LCDB_DB"> (%p)", cdbp);
  else
    lua_pushfstring(L, "<"LCDB_DB"> (closed)");
  return 0;
}

static int lcdbm_get(lua_State *L)
{
  size_t klen;
  unsigned vlen, vpos;
  int ret;
  struct cdb *cdbp = check_cdb(L, 1);
  const char *key = luaL_checklstring(L, 2, &klen);

  ret = cdb_find(cdbp, key, klen);
  if (ret > 0)
  {
    vpos = cdb_datapos(cdbp);
    vlen = cdb_datalen(cdbp);
    lua_pushlstring(L, cdb_get(cdbp, vlen, vpos), vlen);
    return 1;
  }
  else if (ret == 0)
  {
    lua_pushnil(L);
    return 1;
  }
  else /* ret < 0 */
  {
    return push_errno(L, errno);
  }
}

static int lcdbm_find_all(lua_State *L)
{
  unsigned klen;
  int ret;
  int n = 1;
  struct cdb *cdbp = check_cdb(L, 1);
  const char *key = luaL_checklstring(L, 2, &klen);

  struct cdb_find cdbf;
  cdb_findinit(&cdbf, cdbp, key, klen);

  lua_newtable(L);
  while((ret = cdb_findnext(&cdbf)))
  {
    unsigned vpos, vlen;
    if (ret < 0) /* error */
      return push_errno(L, errno);

    vpos = cdb_datapos(cdbp);
    vlen = cdb_datalen(cdbp);
    lua_pushlstring(L, cdb_get(cdbp, vlen, vpos), vlen);
    lua_rawseti(L, -2, n);
    n++;
  }
  return 1;
}
  
static int lcdbm_iternext(lua_State *L)
{
  struct cdb *cdbp = (struct cdb*)lua_touserdata(L, lua_upvalueindex(1));
  unsigned pos = lua_tointeger(L, lua_upvalueindex(2));

  int ret = cdb_seqnext(&pos, cdbp);
  lua_pushinteger(L, pos);
  lua_replace(L, lua_upvalueindex(2));
  if (ret > 0)
  {
    int klen = cdb_keylen(cdbp);
    int kpos = cdb_keypos(cdbp);
    lua_pushlstring(L, cdb_get(cdbp, klen, kpos), klen);
    int vlen = cdb_datalen(cdbp);
    int vpos = cdb_datapos(cdbp);
    lua_pushlstring(L, cdb_get(cdbp, vlen, vpos), vlen);
    return 2;
  }
  else if (ret == 0) /* finished */
  {
    lua_pushnil(L);
    return 1;
  }
  else /* ret < 0, error */
  {
    return push_errno(L, errno);
  }
}

static int lcdbm_iter(lua_State *L)
{
  struct cdb *cdbp = check_cdb(L, 1);

  unsigned pos;
  cdb_seqinit(&pos, cdbp);
  lua_pushinteger(L, pos);
  lua_pushcclosure(L, lcdbm_iternext, 2);
  return 1;
}

static struct cdb_make *push_cdb_make(lua_State *L)
{
  struct cdb_make *cdbmp = (struct cdb_make*)lua_newuserdata(L, sizeof(struct cdb_make));
  luaL_getmetatable(L, LCDB_MAKE);
  lua_setmetatable(L, -2);
  lua_newtable(L);
  lua_setfenv(L, 1);
  return cdbmp;
}

static struct cdb_make *check_cdb_make(lua_State *L, int n)
{
  struct cdb_make *cdbmp = luaL_checkudata(L, n, LCDB_MAKE);
  if (cdb_fileno(cdbmp) < 0)
    luaL_error(L, "attemped to use a closed cdb_make");
  return cdbmp;
}

static int lcdb_make(lua_State *L)
{
  int fd;
  int ret;
  struct cdb_make *cdbmp;
  const char *dest = luaL_checkstring(L, 1);
  const char *tmpname = luaL_checkstring(L, 2);

  fd = open(tmpname, O_RDWR|O_CREAT|O_EXCL, 0666);
  if (fd < 0)
    return push_errno(L, errno);

  cdbmp = push_cdb_make(L);
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

static int lcdbmakem_gc(lua_State *L)
{
  struct cdb_make *cdbmp = luaL_checkudata(L, 1, LCDB_MAKE);

  if (cdbmp->cdb_fd >= 0)
  {
    close(cdbmp->cdb_fd);
    cdbmp->cdb_fd = -1;
    cdb_make_free(cdbmp);
  }
  return 0;
}

static int lcdbmakem_tostring(lua_State *L)
{
  struct cdb_make *cdbmp = luaL_checkudata(L, 1, LCDB_MAKE);

  if (cdbmp->cdb_fd >= 0)
    lua_pushfstring(L, "<"LCDB_MAKE"> (%p)", cdbmp);
  else
    lua_pushfstring(L, "<"LCDB_MAKE"> (closed)");
  return 1;
}

static int lcdbmakem_add(lua_State *L)
{
  static const char *const opts[] = { "add", "replace", "insert", "warn", "replace0", NULL };
  unsigned klen, vlen;
  struct cdb_make *cdbmp = check_cdb_make(L, 1);
  const char *key = luaL_checklstring(L, 2, &klen);
  const char *value = luaL_checklstring(L, 3, &vlen);
  /* by default, add unconditionally */
  int mode = luaL_checkoption(L, 4, "add", opts);

  int ret = cdb_make_put(cdbmp, key, klen, value, vlen, mode);
  if (ret < 0)
    return push_errno(L, errno);
  return 0;
}

static int lcdbmakem_finish(lua_State *L)
{
  struct cdb_make *cdbmp = check_cdb_make(L, 1);
  /* retrieve destination, current filename */
  lua_getfenv(L, -1);
  lua_getfield(L, -1, "dest");
  const char *dest = lua_tostring(L, -1);
  lua_getfield(L, -2, "tmpname");
  const char *tmpname = lua_tostring(L, -1);
  lua_pop(L, 3);

  if (cdb_make_finish(cdbmp) < 0 || fsync(cdb_fileno(cdbmp)) < 0 || 
      close(cdb_fileno(cdbmp)) < 0 || rename(tmpname, dest) < 0)
  {
    cdbmp->cdb_fd = -1; // fatal errors, already freed cdbmp
    return push_errno(L, errno);
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
  {"iter", lcdbm_iter},
  {NULL, NULL}
};

static const struct luaL_Reg lcdbmake_m [] = {
  {"__gc", lcdbmakem_gc},
  {"__tostring", lcdbmakem_tostring},
  {"add", lcdbmakem_add},
  {"finish", lcdbmakem_finish},
  {NULL, NULL}
};

int luaopen_cdb(lua_State *L)
{
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
