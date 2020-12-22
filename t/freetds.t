# vim:set ft= ts=4 sw=4 et:

use t::Test;

no_root_location();

run_tests;

__DATA__

=== TEST 1: connect db using charset option (utf8)
--- server_config
 location /t {
        content_by_lua_block {
            local freetds = require "resty.freetds"
            local cjson   = require "cjson"
            local db = freetds:new()

            db:set_timeout(60) -- 1 sec

            local ok, err, errno, sqlstate = db:connect({
                login_time = 60,
                timeout = 60,
                connect_config = {
                host     = "xxx",
                port     =  11433,
                database = "ZK_PZJK",
                user     = "sa",
                password = "xxx",
                charset  = "utf8",
                }})

            if not ok then
                ngx.say("failed to connect1: ", err, ": ", errno, " ", sqlstate)
                return
            end

            -- generate test data
            local res, fields, err, errno, sqlstate = db:query("select * from dbo.t_account")
            if not res then
                ngx.say("bad result2: ", err, ": ", errno, ": ", sqlstate, ".")
                return
            end

            ngx.say("data: ",#res)
            ngx.say("fields: ", cjson.encode(fields))
            db:close()
        }
    }

--- request
GET /t
--- response_body
[{"id":"1","name":"愛麗絲"}]
--- timeout: 300
