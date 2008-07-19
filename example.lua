#! /usr/bin/env lua

-- lua-tinycdb does not assign to any global variables, so name it yourself
cdb = require("cdb")

-- make new cdb, creating a file called example.cdb.tmp that will be renamed 
-- to example.cdb when finished. Due to the semantics of rename, this will 
-- atomically replace example.cdb with the newly built database
maker = assert(cdb.make("example.cdb", "example.cdb.tmp"))

maker:add("key", "value")
maker:add("foo", "bar")
-- note it is possible to have multiple values for the same key
maker:add("foo", "baz")
maker:add("baz", "qux")

-- a 3rd argument can be given to maker:add() that controls the behaviour when 
-- adding a key that already exists:
--  * "add": the default. No duplicate checking will be performed.
--
--  * "replace": If the key already exists, it will be removed from the  
--  database before adding new key,value pair. This requires moving data in 
--  the file, and can be quite slow if the file is large. All matching old 
--  records will be removed this way.  
--
--  * "replace0": If the key already exists and it isn't the last record in 
--  the file, old record will be zeroed out before adding new key,value   
--  pair. This is alot faster than CDB_PUT_REPLACE, but some extra data will  
--  still be present in the file. The data -- old record -- will not be  
--  accessible by normal searches, but will appear in sequential database 
--  traversal. 
--
--  * "insert": add key,value pair only if such a key does not exists in the 
--  database.

-- renames example.cdb.tmp to example.cdb and closes file
assert(maker:finish())

-- open our new database
db = cdb.open("example.cdb")

local var = db:get("key") -- "value"
local var2 = db:get("baz") -- "qux"
local var3 = db:get("foo") -- "bar", as get() returns the first value added

-- outputs:
--
-- key  value
-- foo  bar
-- foo  baz
-- baz  qux
for k, v in db:iter() do
  print(k, v)
end

-- find_all returns a table of all values for the given key
local t = db:find_all("foo") -- t = { "bar", "baz" }

db:close()
