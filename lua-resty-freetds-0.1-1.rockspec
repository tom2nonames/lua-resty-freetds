rockspec_format = "3.0"
package = "lua-resty-freetds"
version = "0.1-1"
source = {
   url = "git+https://github.com/tom2nonames/lua-resty-freetds.git"
}
description = {
   detailed = "Nonblocking Lua MS-SQL driver library for ngx_lua or OpenResty",
   homepage = "https://github.com/tom2nonames/lua-resty-freetds",
   license = "BSD License 2.0",
   labels = { "Freetds", "OpenResty", "Cosocket", "Nginx" }
}
build = {
   type = "builtin",
   modules = {
      ["resty.freetds"] = "lib/resty/freetds.lua",
      ["resty.freetds.result"] = "lib/resty/freetds/result.lua"
   }
}
