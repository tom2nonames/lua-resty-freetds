local bit = require("bit")
local blshift= bit.lshift
local bbor   = bit.bor
local sfmt = string.format

local ngx = ngx
local ngx_log = ngx.log
local ngx_DEBUG = ngx.DEBUG

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local ffi, C
local ffi_string
local ffi_sizeof
local ffi_cast
local ffi_new

local _M = { _VERSION = '0.1' }


local mt = { __index = _M }
local db_type =
    {
    SYBCHAR = 47,		--/* 0x2F */
	SYBVARCHAR = 39,	--/* 0x27 */
	SYBINTN = 38,		--/* 0x26 */
	SYBINT1 = 48,		--/* 0x30 */
	SYBINT2 = 52,		--/* 0x34 */
	SYBINT4 = 56,		--/* 0x38 */
	SYBINT8 = 127,		--/* 0x7F */
	SYBFLT8 = 62,		--/* 0x3E */
	SYBDATETIME = 61,	--/* 0x3D */
	SYBBIT = 50,		--/* 0x32 */
	SYBBITN = 104,		--/* 0x68 */
	SYBTEXT = 35,		--/* 0x23 */
	SYBNTEXT = 99,		--/* 0x63 */
	SYBIMAGE = 34,		--/* 0x22 */
	SYBMONEY4 = 122,	--/* 0x7A */
	SYBMONEY = 60,		--/* 0x3C */
	SYBDATETIME4 = 58,	--/* 0x3A */
	SYBREAL = 59,		--/* 0x3B */
	SYBBINARY = 45,		--/* 0x2D */
	SYBVOID = 31,		--/* 0x1F */
	SYBVARBINARY = 37,	--/* 0x25 */
	SYBNUMERIC = 108,	--/* 0x6C */
	SYBDECIMAL = 106,	--/* 0x6A */
	SYBFLTN = 109,		--/* 0x6D */
	SYBMONEYN = 110,	--/* 0x6E */
	SYBDATETIMN = 111,	--/* 0x6F */
	SYBNVARCHAR = 103,	--/* 0x67 */
	SYBDATE = 49,		--/* 0x31 */
	SYBTIME = 51,		--/* 0x33 */
	SYBBIGDATETIME = 187,	--/* 0xBB */
	SYBBIGTIME = 188,	--/* 0xBC */
	SYBMSDATE = 40,		--/* 0x28 */
	SYBMSTIME = 41,		--/* 0x29 */
	SYBMSDATETIME2 = 42,	--/* 0x2A */
	SYBMSDATETIMEOFFSET = 43, --/* 0x2B */
}

function _M.new(self, cwrap)

    local opts = {
        symbolize_keys = false,
        as_array = false,
        cache_rows = true,
        first = false,
        timezone = "local",
        empty_sets = true
    }

    ffi = cwrap.ffi
    ffi_string = ffi.string
    ffi_sizeof = ffi.sizeof
    ffi_cast   = ffi.cast
    ffi_new    = ffi.new
    C   = cwrap.C

    local wrapper = {
        cwrap = cwrap,
        client = cwrap.client,
        local_offset = nil,
        fields = {},
        fields_processed = {},
        results = nil,
        encoding = nil,
        dbresults_retcodes = {},
        number_of_results = 0,
        number_of_fields = 0,
        number_of_rows = 0,
        opts = opts
    }
    return setmetatable(wrapper, mt)
end

function _M.return_code(self)

    local ret = nil
    ret = self.dbresults_retcodes[self.number_of_results]
    if not ret then
        ret = self.cwrap._dbresults(self.client)
        self.dbresults_retcodes[self.number_of_results] = ret
    end
    return ret
end

function _M.fetch_row(self, timezone, symbolize_keys, as_array )

    local client = self.client
    local row = new_tab(0, self.number_of_fields)
    local case = {}

    case[db_type.SYBINT1] = function(data, data_len, col_type)
        local val = ffi_cast("DBTINYINT *", data)
        return val[0]
    end

    case[db_type.SYBINT2] = function(data, data_len, col_type)
        local val = ffi_cast("DBSMALLINT *", data)
        return val[0]
    end

    case[db_type.SYBINT4] = function(data, data_len, col_type)
        local val = ffi_cast("DBINT *", data)
        return val[0]
    end

    case[db_type.SYBINT8] = function(data, data_len, col_type)
        local val = ffi_cast("DBBIGINT *", data)
        return val[0]
    end

    case[db_type.SYBBIT] = function(data, data_len, col_type)
        local val = ffi_cast("int *", data)
        val = val[0]
        return val == 1 and true or false
    end

    case[db_type.SYBNUMERIC] = function(data, data_len, col_type)
        local data_info = ffi_new("DBTYPEINFO *")
        data_info = C.dbcoltypeinfo(client, col)

        local data_slength = data_info.precision + data_info.scale + 1

        local converted_decimal = ffi_new("char[?]", data_slength)
        converted_decimal = ffi_cast("BYTE *", converted_decimal)
        C.dbconvert(client, col_type, data, data_len, db_type.SYBVARCHAR, converted_decimal, -1)
        converted_decimal = ffi_cast("char *", converted_decimal)
        return ffi_string(converted_decimal, data_slength)
    end

    case[db_type.SYBDECIMAL] = case[db_type.SYBNUMERIC]

    case[db_type.SYBFLT8] = function(data, data_len, col_type)
        local val = ffi_cast("double *", data)
        return val[0]
    end

    case[db_type.SYBREAL] = function(data, data_len, col_type)
        local val = ffi_cast("float *", data)
        return val[0]
    end

    case[db_type.SYBMONEY] = function(data, data_len, col_type)
        local money = ffi_cast("DBMONEY *", data)
        local converted_money = ffi_new("char[?]",25)
        local money_value = ffi_cast("long long", money.mnyhigh)
              money_value = blshift(money_value, 32)
              money_value = bbor(money_value, money.mnylow)
        ffi.C.sprintf(converted_money, "%lld", money_value)

        return ffi_string(converted_money)
    end

    case[db_type.SYBMONEY4] = function(data, data_len, col_type)
        local money = ffi_cast("DBMONEY *", data)
        return sfmt("%f", money.mny4 / 10000.0)
    end

    case[db_type.SYBBINARY] = function(data, data_len, col_type)
        return ffi_string(data, data_len)
    end

    case[db_type.SYBIMAGE] = case[db_type.SYBBINARY]

    --SYBUNIQUE
    case[36] = function(data, data_len, col_type)
        local converted_unique = ffi_new("char[?]",25)
        converted_unique = ffi_cast("BYTE *", converted_unique)
        C.dbconvert(client, col_type, data, 37, db_type.SYBVARCHAR, converted_unique, -1)
        return ffi_string(converted_unique)
    end


    case[db_type.SYBDATETIME4] = function(data, data_len, col_type)
        local new_data = ffi_new("DBDATETIME")
        local size =  ffi_sizeof(new_data)
        new_data = ffi_cast("BYTE *", new_data)
        C.dbconvert(client, col_type, data, 37, db_type.SYBDATETIME, new_data, size)
        data  = new_data
        data_len = size
        local dr = ffi_new("DBDATEREC")
        dr = ffi_cast("BYTE *", dr)
        C.dbdatecrack(client, dr, data)
        if dr.year + dr.month  + dr.day +
           dr.hour + dr.minute + dr.second +
           dr.millisecond ~= 0 then
            return sfmt("%04d-%02d-%02dT%02d:%02d:%02d.%03d",
                        dr.year, dr.month,  dr.day,
                        dr.hour, dr.minute, dr.second,
                        dr.millisecond)
        end
        return ngx.null
    end

    case[db_type.SYBDATETIME] = function(data, data_len, col_type)
        local dr = ffi_new("DBDATEREC")
              dr = ffi_cast("DBDATEREC *", dr)

        data = ffi_cast("DBDATETIME *", data)
        local ret = C.dbdatecrack(client, dr, data)
        if dr.year + dr.month  + dr.day +
           dr.hour + dr.minute + dr.second +
           dr.millisecond ~= 0 then
            --local timezone = dr.tzone * 60
            --local h, m = math.modf(timezone / 3600)
            return sfmt("%04d-%02d-%02dT%02d:%02d:%02d.%03d", --%+.2d:%02d",
                        dr.year, dr.month,  dr.day,
                        dr.hour, dr.minute, dr.second,
                        dr.millisecond) --, h, m )
        end
        return ngx.null
    end

    case[40] = function(data, data_len, col_type)
        if C.dbtds(client) >= 11 then
            local dr2 = ffi_new("DBDATEREC2")
            dr2 = ffi_cast("DBDATEREC2 *")
            C.dbanydatecrack(client, dr2, col_type, data)
            local conv = {
                [40] = function(dr2)
                    return sfmt("%04d-%02d-%02d",
                                dr2.year, dr2.month,  dr2.day)
                end,

               [41] = function(dr2)

                    return sfmt("%02d:%02d:%02d.%03d",
                                dr2.hour, dr2.minute, dr2.second,
                                dr2.nanosecond/1000000)
               end,

               [42] = function(dr2)
                local timezone = dr2.tzone * 60
                local h, m = math.modf(timezone / 3600)
                return sfmt("%04d-%02d-%02dT%02d:%02d:%02d.%03d%+.2d:%02d",
                            dr2.year, dr2.month,  dr2.day,
                            dr2.hour, dr2.minute, dr2.second,
                            dr2.nanosecond/1000000, h, m)
               end,

               [43] = function(dr2)
                local timezone = dr2.tzone * 60
                local h, m = math.modf(timezone / 3600)
                return sfmt("%04d-%02d-%02dT%02d:%02d:%02d.%09d%+.2d:%02d",
                            dr2.year, dr2.month,  dr2.day,
                            dr2.hour, dr2.minute, dr2.second,
                            dr2.nanosecond, h, m)
               end
            }

            local conv_func = conv[col_type]

            return conv_func(dr2)

        else
            return ffi_string(data, data_len)
        end
    end

    case[41] = case[40]
    case[42] = case[40]
    case[43] = case[40]

    case[db_type.SYBCHAR] = function(data, data_len, col_type)
        return ffi_string(data, data_len)
    end

    case[db_type.SYBCHAR] = case[db_type.SYBCHAR]

    case[db_type.SYBTEXT] = function(data, data_len, col_type)
        return ffi_string(data, data_len)
    end

    --SYBVARIANT
    case[98] = function(data, data_len, col_type)
        if data_len == 4 then
            val = ffi_cast("DBINT *", data)
            return val[0]
        else
            return ffi_string(data, data_len)
        end
    end

    for i = 0, self.number_of_fields - 1 do
        local val = nil
        local col = i + 1
        local col_type = C.dbcoltype(client, col)

        local data = ffi_new("BYTE *")
        data  = C.dbdata(client, col)
        local data_len = C.dbdatlen(client, col)

        local isnull = ffi_cast("void *", data) <= nil and data_len == 0
        --ngx_log(ngx_DEBUG, "~~~ ", isnull,", data_len: ", data_len,", col_type: ", col_type)
        if isnull then
            val = ngx.null
        else
            local func = case[col_type]

            if not func then
                ngx_log(ngx_DEBUG, "col_type: ", col_type)
                val = ffi_string(data, data_len)
            end

            val = func(data, data_len, col_type)
        end

        if as_array then
            row[i+1] = val
            --ngx_log(ngx_DEBUG,"data as array.")
        else
            local key
            if self.number_of_results == 0 then
               key = self.fields[i + 1]
            else
               key = self.fields[self.number_of_results + 1]
               key = key[i + 1]
            end
            --ngx_log(ngx_DEBUG, "->row[",key,"] = ", cjson.encode(val))
            row[key]= val
        end
    end
    return row
end

function _M.getfields(self)
    local client = self.client
    local dbsqlok_rc   = self.cwrap.db_sql_ok(client)
    local dbresults_rc = self:return_code()

    local fields_processed = self.fields_processed[self.number_of_results]

    if dbsqlok_rc == 1 and dbresults_rc == 1 and fields_processed == nil then
        local symbolize_keys = self.opts.symbolize_keys

        self.number_of_fields = C.dbnumcols(client)
        ngx_log(ngx_DEBUG, " number of fileds: ", self.number_of_fields)
        if self.number_of_fields > 0 then

            local fields = new_tab(0, self.number_of_fields)
            for fldi = 0, self.number_of_fields - 1 do
                local colname = ffi_new("char *")
                      colname = C.dbcolname(client, fldi + 1)
                local field = ffi_string(colname)
                fields[fldi + 1] = field
                --ngx_log(ngx_DEBUG, "-> field[",fldi+1,"] = ", field )
            end

            if self.number_of_results == 0 then
                self.fields = fields
            elseif self.number_of_results == 1 then
                local multi_rs_fields = {}
                multi_rs_fields[0] = self.fields
                multi_rs_fields[1] = fields
                self.fields = multi_rs_fields
            else
                self.fields[self.number_of_results] = fields
            end
        end

        self.fields_processed[self.number_of_results] = true
    end

    return self.fields
end

--[[
{
frist = false,
symbolize_keys = false,
as_array = false,
cache_rows = false,
timezone =
empty_sets = false
}
--]]
function _M.each(self, opts)
    local client = self.client
    local cwrap = self.cwrap

    if opts then
        for k , v in pairs(opts) do
            --ngx_log(ngx_DEBUG, k, "= ", v, ", old: ",self.opts[k])
            self.opts[k] = v
        end
    end

    local first , symbolize_keys , as_array , cache_rows , timezone, empty_sets
        = self.opts.first, self.opts.symbolize_keys, self.opts.as_array or cwrap.is_compact_arrays,
          self.opts.cache_rows, self.opts.timezone, self.opts.empty_sets

    ngx_log(ngx_DEBUG, "opts: first = " , first ,
                       ", symbolize_keys = ", symbolize_keys,
                       ", as_array = ", as_array,
                       ", cache_row = ", cache_rows,
                       ", timezone = ", timezone,
                       ", empty_sets = ", empty_sets)

    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *", userdata)

    if not self.results then
        local dbsqlok_rc   = self.cwrap.db_sql_ok(client)
        local dbresults_rc = self:return_code()
        ngx_log(ngx_DEBUG, "db_sql_ok:  " , dbsqlok_rc , ", db results:", dbresults_rc)
        self.results = {}

        while( dbsqlok_rc == 1 and dbresults_rc == 1 ) do
            local has_rows = C.dbrows(client) == 1 and true or false
            if has_rows or empty_sets or self.number_of_results == 0 then
                self:getfields()
            end
            if has_rows or empty_sets and self.number_of_fields > 0 then
                local rowi = 0
                local result = {}
                while (cwrap._dbnextrow(client) ~= -2) do
                    local row = self:fetch_row(timezone, symbolize_keys, as_array)
                    --ngx_log(ngx_DEBUG, "index: ",rowi, " ", cjson.encode(row))
                    if cache_rows then
                        result[rowi+1] = row
                        --table.insert(result, rowi+1, row)
                    end
                    if first then
                        ngx_log(ngx_DEBUG, "only read first row.")
                        C.dbcanquery(client)
                        userdata.dbcancel_sent = true
                    end
                    rowi = rowi + 1
                end
                --ngx_log(ngx_DEBUG, cjson.encode(result))

                self.number_of_rows = rowi

                if cache_rows then
                    if self.number_of_results == 0 then
                        self.results = result
                    elseif self.number_of_results == 1 then
                        local multi_resultsets = {}
                        multi_resultsets[0] = self.results
                        multi_resultsets[1] = result
                        self.results = multi_resultsets
                    else
                        self.results[self.number_of_results] = result
                    end
                end

                self.number_of_results = self.number_of_results + 1
                dbresults_rc = self:return_code()
                self.fields_processed[self.number_of_results] = nil
            else
                dbresults_rc = cwrap.db_sql_ok(client)
                self.dbresults_retcodes[self.number_of_results] = dbresults_rc
                self.fields_processed[self.number_of_results] = nil
            end
        end
        if dbresults_rc == 0 then
            ngx_log(ngx.WARN,"Something in the dbresults() while loop set the return code to FAIL.\n")
        end
    end

    return self.results, self.fields
end

function _M.cancel(self)
    local client = self.client
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)

    if client and userdata.dbcancel_sent then
        self.cwrap.db_sql_ok(client)
        C.dbcancel(client)
        userdata.dbcancel_sent = true
        userdata.dbsql_sent = false
    end

    return true
end

function _M.exec(self)
    local client = self.client
    if client then
        self.cwrap.db_exec(client)
        return C.dbcount(client)
    else
        return nil
    end

end

function _M.affected_row(self)
    local client = self.client
    if client then
        return C.dbcount(client)
    else
        return nil
    end
end

function _M.insert(self)
    local client = self.client
    local cwrap = self.cwrap
    local identity
    if client then
        cwrap.db_exec(client)
        C.dbcmd(client, cwrap.identity_insert_sql)
        if cwrap._dbsqlexec(client) ~= 0 and
           cwrap._dbresults(client) ~= 0 and
           C.dbrows(client) ~= 0 then
            while( cwrap._dbnextrow(client) ~= -2 ) do
                local col = 1
                local data = ffi_new("BYTE *")
                      data = C.dbdata(client, col)
                local data_len = C.dbdatlen(client, col)

                local null_val = data == nil and data_len == 0
                if not null_val then
                    identity = ffi_cast("DBBITINT *", data)
                    identity = identity[0]
                end
            end
        end
        return identity
    else
        return nil
    end
end

return _M