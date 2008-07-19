package="lua-tinycdb"
version="0.1-1"
source = {
   url = "http://luaforge.net/frs/download.php/3046/lua-tinycdb-0.1.tar.gz",
   md5 = "f03b8ae5dec57a8ad7454c554f2f6aeb",
}
description = {
   summary = "Provides support for creating and reading constant databases",
   homepage = "http://asbradbury.org/projects/lua-tinycdb/",
   license = "MIT/X11"
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "make",
   install_pass = false,
   build_variables = {
      LUAINC = "$(LUA_INCDIR)",
      LUALIB = "$(LUA_LIBDIR)",
      LUABIN = "$(LUA_BINDIR)"
   },
   install = {
      lib = { "cdb.so" },
   }
}
