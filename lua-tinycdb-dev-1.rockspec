package="lua-tinycdb"
version="dev-1"
source = {
   url = ""
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
   type = "module",
   modules = {
      cdb = {
         "cdb_find.c",
         "cdb_findnext.c",
         "cdb_hash.c",
         "cdb_init.c",
         "cdb_make_add.c",
         "cdb_make.c",
         "cdb_make_put.c",
         "cdb_seek.c",
         "cdb_seq.c",
         "cdb_unpack.c",
         "lcdb.c"
      }
   }
}
