require("lunit")

local cdb = require("cdb")
local db_name = "test.cdb"

local db = assert(cdb.make(db_name, db_name..".tmp"))
db:add("one", "1")
db:add("two", "2")
db:add("three", "4") -- oops
db:add("three", "3", "replace")
db:add("three", "III")
assert(db:finish())

module("querying a cdb", lunit.testcase, package.seeall)
do
  function setup()
    db = assert(cdb.open(db_name))
  end

  function test_get()
    assert_equal("1", db:get("one"))
    assert_equal("3", db:get("three"))
    assert_nil(db:get("four"))
  end

  function test_pairs()
    local expected_keys = { "one", "two", "three", "three" }
    local expected_values = { "1", "2", "3", "III" }
    local i = 1
    for k, v in db:pairs() do
      assert_equal(expected_keys[i], k)
      assert_equal(expected_values[i], v)
      i = i+1
    end
  end

  function test_findall()
    local t = db:find_all("three")
    assert_equal(2, #t)
    assert_equal("3", t[1])
    assert_equal("III", t[2])
  end

  function test_closed_cdb()
    db:close()
    assert_error(nil, function() db:get("one") end)
  end
end
