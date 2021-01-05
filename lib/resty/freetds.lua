

local ffi = require("ffi")
local results = require("resty.freetds.result")


local ffi_cast   = ffi.cast
local ffi_new    = ffi.new
local ffi_copy   = ffi.copy
local ffi_string = ffi.string

local C = ffi.load(ffi.os == "Windows" and "sybdb" or "sybdb")

ffi.cdef[[

    int sprintf(char *buf, const char *fmt, ...);

    typedef int BOOL;
    typedef short SHORT;

    typedef unsigned char BYTE;
    typedef int STATUS;
    typedef int RETCODE;

    typedef unsigned char DBBOOL;
    typedef char DBCHAR;
    typedef unsigned char DBBIT;
    typedef unsigned char DBTINYINT;
    typedef int16_t DBSMALLINT;
    typedef int32_t DBINT;
    typedef int64_t DBBIGINT;
    typedef unsigned char DBBINARY;
    typedef uint16_t DBUSMALLINT;
    typedef uint32_t DBUINT;
    typedef uint64_t DBUBIGINT;

    typedef struct
    {
        DBSMALLINT len;
        char  str[256];
    } DBVARYCHAR;

    typedef struct
    {
        DBSMALLINT len;
        unsigned char  array[256];
    } DBVARYBIN;

    typedef struct
    {
        unsigned char precision;
        unsigned char scale;
        unsigned char array[33];
    } DBNUMERIC;

    typedef DBNUMERIC DBDECIMAL;

    typedef struct
    {
        DBINT mnyhigh;
        DBUINT mnylow;
    } DBMONEY;

    typedef struct
    {
        DBINT mny4;
    } DBMONEY4;

    typedef struct
    {
        DBINT dtdays;
        DBINT dttime;
    } DBDATETIME;

    typedef struct
    {
        DBUSMALLINT days;
        DBUSMALLINT minutes;
    } DBDATETIME4;

    typedef struct
    {
        DBUBIGINT  time;
        DBINT      date;
        DBSMALLINT offset;
        DBUSMALLINT time_prec:3;
        DBUSMALLINT _res:10;
        DBUSMALLINT has_time:1;
        DBUSMALLINT has_date:1;
        DBUSMALLINT has_offset:1;
    } DBDATETIMEALL;

    typedef struct
    {
        DBUSMALLINT numdays;
        DBUSMALLINT nummins;
    } DBDATETIM4;

    typedef struct dbtypeinfo
    {
        DBINT precision;
        DBINT scale;
    } DBTYPEINFO;

    struct dbstring
    {
        BYTE *strtext;
        DBINT strtotlen;
        struct dbstring *strnext;
    };
    typedef struct dbstring DBSTRING;

    /* Used by dbcolinfo */
    enum
    { MAXCOLNAMELEN = 512 }; /* duplicates TDS_SYSNAME_SIZE */
    typedef enum { CI_REGULAR=1, CI_ALTERNATE=2, CI_CURSOR=3 } CI_TYPE;

    typedef struct
    {
        DBINT SizeOfStruct;
        DBCHAR  Name[MAXCOLNAMELEN+2];
        DBCHAR  ActualName[MAXCOLNAMELEN+2];
        DBCHAR  TableName[MAXCOLNAMELEN+2];
        SHORT Type;
        DBINT UserType;
        DBINT MaxLength;
        BYTE  Precision;
        BYTE  Scale;
        BOOL  VarLength;
        BYTE  Null;
        BYTE  CaseSensitive;
        BYTE  Updatable;
        BOOL  Identity;

    } DBCOL;

    typedef struct
    {
        DBINT SizeOfStruct;
        DBCHAR  Name[MAXCOLNAMELEN+2];
        DBCHAR  ActualName[MAXCOLNAMELEN+2];
        DBCHAR  TableName[MAXCOLNAMELEN+2];
        SHORT Type;
        DBINT UserType;
        DBINT MaxLength;
        BYTE  Precision;
        BYTE  Scale;
        BOOL  VarLength;
        BYTE  Null;
        BYTE  CaseSensitive;
        BYTE  Updatable;
        BOOL  Identity;
        SHORT ServerType;
        DBINT ServerMaxLength;
        DBCHAR  ServerTypeDeclaration[256];
    } DBCOL2;

    typedef struct {
        short int is_set;
        int is_message;
        int cancel;
        char error[1024];
        char source[1024];
        int severity;
        int dberr;
        int oserr;
      } errordata;

    typedef struct {
        short int closed;
        short int timing_out;
        short int dbsql_sent;
        short int dbsqlok_sent;
        RETCODE dbsqlok_retcode;
        short int dbcancel_sent;
        short int nonblocking;
        errordata nonblocking_error;
        void  *message_handler;
      } client_userdata;

    struct tds_microsoft_dbdaterec
      {
          DBINT year;		/* 1753 - 9999  	   */
          DBINT quarter;		/* 1 - 4 		   */
          DBINT month;		/* 1 - 12 		   */
          DBINT day;		/* 1 - 31 		   */
          DBINT dayofyear;	/* 1 - 366 		   */
          DBINT week;            	/* 1 - 54 (for leap years) */
          DBINT weekday;		/* 1 - 7 (Mon. - Sun.)     */
          DBINT hour;		/* 0 - 23 		   */
          DBINT minute;		/* 0 - 59 		   */
          DBINT second;		/* 0 - 59 		   */
          DBINT millisecond;	/* 0 - 999 		   */
          DBINT tzone;		/* -840 - 840		   */
      };

    struct tds_microsoft_dbdaterec2
      {
          DBINT year;		/* 1753 - 9999  	   */
          DBINT quarter;		/* 1 - 4 		   */
          DBINT month;		/* 1 - 12 		   */
          DBINT day;		/* 1 - 31 		   */
          DBINT dayofyear;	/* 1 - 366 		   */
          DBINT week;            	/* 1 - 54 (for leap years) */
          DBINT weekday;		/* 1 - 7 (Mon. - Sun.)     */
          DBINT hour;		/* 0 - 23 		   */
          DBINT minute;		/* 0 - 59 		   */
          DBINT second;		/* 0 - 59 		   */
          DBINT nanosecond;	/* 0 - 999999999	   */
          DBINT tzone;		/* 0 - 127  (Sybase only)  */
      };

    typedef struct tds_microsoft_dbdaterec  DBDATEREC;
    typedef struct tds_microsoft_dbdaterec2 DBDATEREC2;

    RETCODE dbinit(void);

    typedef struct {} LOGINREC;

    typedef struct {} DBPROCESS;

    int dbtds(DBPROCESS * dbprocess);

    LOGINREC *dblogin(void);

    void dbloginfree(LOGINREC * login);

    void dbfreebuf(DBPROCESS * dbproc);

    RETCODE dbuse(DBPROCESS * dbproc, const char *name);

    DBPROCESS *dbopen(LOGINREC * login, const char *server);

    void dbclose(DBPROCESS * dbproc);

    void dbexit(void);

    DBBOOL dbdead(DBPROCESS * dbproc);

    RETCODE dbcancel(DBPROCESS * dbproc);

    RETCODE dbcmd(DBPROCESS * dbproc, const char cmdstring[]);

    RETCODE dbresults(DBPROCESS * dbproc);

    int dbnumcols(DBPROCESS * dbproc);

    char *dbcolname(DBPROCESS * dbproc, int column);

    int dbcoltype(DBPROCESS * dbproc, int column);

    DBTYPEINFO *dbcoltypeinfo(DBPROCESS * dbproc, int column);

    DBINT dbcollen(DBPROCESS * dbproc, int column);

    DBBOOL dbwillconvert(int srctype, int desttype);

    RETCODE dbbind(DBPROCESS * dbproc, int column, int vartype, DBINT varlen, BYTE * varaddr);

    RETCODE dbnullbind(DBPROCESS * dbproc, int column, DBINT * indicator);

    RETCODE dbrows(DBPROCESS * dbproc);

    BYTE *dbdata(DBPROCESS * dbproc, int column);

    DBINT dbdatlen(DBPROCESS * dbproc, int column);

    RETCODE dbcanquery(DBPROCESS * dbproc);

    STATUS dbnextrow(DBPROCESS * dbproc);

    RETCODE dbsqlok(DBPROCESS * dbproc);

    RETCODE dbsqlsend(DBPROCESS * dbproc);

    DBBOOL dbhasretstat(DBPROCESS * dbproc);

    DBINT dbretstatus(DBPROCESS * dbproc);

    RETCODE dbsetlversion (LOGINREC * login, BYTE version);

    RETCODE dbsetlogintime(int seconds);

    RETCODE dbsetlname(LOGINREC * login, const char *value, int which);
    RETCODE dbsetlbool(LOGINREC * login, int value, int which);
    RETCODE dbsetlshort(LOGINREC * login, int value, int which);
    RETCODE dbsetllong(LOGINREC * login, long value, int which);

    RETCODE dbsetopt(DBPROCESS * dbproc, int option, const char *char_param, int int_param);

    RETCODE dbsettime(int seconds);

    void dbsetuserdata(DBPROCESS * dbproc, BYTE * ptr);

    BYTE *dbgetuserdata(DBPROCESS * dbproc);

    RETCODE dbdatecrack(DBPROCESS * dbproc, DBDATEREC * di, DBDATETIME * dt);

    DBINT dbconvert(DBPROCESS * dbproc, int srctype, const BYTE * src, DBINT srclen, int desttype, BYTE * dest, DBINT destlen);

    typedef int (*EHANDLEFUNC) (DBPROCESS * dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr);

    typedef int (*MHANDLEFUNC) (DBPROCESS * dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname,
			    char *proc, int line);

    EHANDLEFUNC dberrhandle(EHANDLEFUNC handler);
    MHANDLEFUNC dbmsghandle(MHANDLEFUNC handler);
]];

local ngx = ngx
local ngx_log = ngx.log
local ngx_DEBUG = ngx.DEBUG
local ngx_WARN  = ngx.WARN

local sfmt = string.format

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = { _VERSION = '0.1' }


local mt = { __index = _M }

--[[
    host
    port
    database
    user
    passwrod
    charset

]]
local _t_db_version = {
    ['unknown'] = 0,
    ['46'     ] = 1,
    ['100'    ] = 2,
    ['42'     ] = 3,
    ['70'     ] = 4,
    ['7.0'    ] = 4,
    ['71'     ] = 5,
    ['7.1'    ] = 5,
    ['80'     ] = 5,
    ['8.0'    ] = 5,
    ['72'     ] = 6,
    ['7.2'    ] = 6,
    ['90'     ] = 6,
    ['9.0'    ] = 6,
    ['73'     ] = 7,
    ['7.3'    ] = 7
}

local _t_db_set_name = {
    host     = { which = 1, func = C.dbsetlname },
    user     = { which = 2, func = C.dbsetlname },
    password = { which = 3, func = C.dbsetlname },
    hid      = { which = 4, func = C.dbsetlname },
    app      = { which = 5, func = C.dbsetlname },
    bcp      = { which = 6, func = C.dbsetlbool },
    lang     = { which = 7, func = C.dbsetlname },
    noshort  = { which = 8, func = C.dbsetlbool },
    --hier     = { which = 9, func = C.dbsetlshort },
    charset  = { which = 10, func = C.dbsetlname },
    packet   = { which = 11, func = C.dbsetllong },
    encrypt  = { which = 12, func = C.dbsetlbool },
    labeled  = { which = 13, func = C.dbsetlbool },
    database = { which = 14, func = C.dbsetlname },
    net_work_auth    = { which = 101, func = C.dbsetlbool },
    mutual_auth      = { which = 102, func = C.dbsetlbool },
    server_principal = { which = 103, func = C.dbsetlname },
    utf16       = { which = 1001, func = C.dbsetlbool },
    ntlmv2      = { which = 1002, func = C.dbsetlbool },
    readonly    = { which = 1003, func = C.dbsetlbool },
    delegation  = { which = 1004, func = C.dbsetlbool },
}

local function setup(name, value, login)
    local item = _t_db_set_name[name]

    if item then
        local func, which = item.func, item.which
        return func(login, value, which)
    end

    return nil, "No matching settings were found. item: " .. name
end
--raise_error(DBPROCESS *dbproc, int is_message, int cancel, const char *error, const char *source, int severity, int dberr, int oserr)
local function raise_error(dbproc, is_message, cancel, error_str, source_str, severity, dberr, oserr)

    local client = ffi_cast("DBPROCESS *", dbproc)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *", userdata)

    local userdata_is_null = ffi_cast("void *", userdata) <= nil
    if cancel and not C.dbdead(client) and not userdata_is_null and userdata.closed == 0 then

        userdata.dbsqlok_sent  = true
        C.dbsqlok(client)
        userdata.dbcancel_sent = true
        C.dbcancel(client)
    end

    if severity <= 10 and is_message then

        local message_handler = (not userdata_is_null and userdata.message_handler ) and userdata.message_handler or nil
        if message_handler and message_handler ~= nil then
            ngx_log(ngx_DEBUG, "set callback function")
        end
        return nil
    end

    local err_string_format = "source: %s, error: %s,\n serverity: %d, db err no: %d, os err no: %d"
    local msg = sfmt(err_string_format, ffi_string(source_str), ffi_string(error_str), severity, dberr, oserr)

    error(msg)

    return nil
end

--DBPROCESS *dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr)
local function err_hander(dbproc, severity, dberr, oserr, dberrstr, oserrstr)
        ngx_log(ngx_DEBUG, "===1")
        local source = "error"
        local return_value =  2
        local cancel = false
        local client = ffi_cast("DBPROCESS *", dbproc)
        local userdata = ffi_new("client_userdata *")
              userdata = C.dbgetuserdata(client)
              userdata = ffi_cast("client_userdata *",userdata)

        local SYBEVERDOWN   = 100
        local SYBESEOF      = 20017
        local SYBESMSG      = 20018
        local SYBEICONVI    = 2403

        local SYBEICONVO    = 2402
        local SYBETIME      = 20003
        local SYBEWRIT      = 20006

        if (dberr == SYBEVERDOWN or dberr == SYBESEOF or
            dberr == SYBESMSG    or dberr == SYBEICONVI ) then
            return return_value
        end

        if dberr == SYBEICONVO then
            C.dbfreebuf(client)
            return return_value
        end

        if dberr == SYBETIME then
            return_value = 3 --INT_TIMEOUT
            cancel = true
        end

        local userdata_is_null = ffi_cast("void *", userdata) <= nil
        if dberr == SYBEWRIT then
            if ( not userdata_is_null and userdata.dbsqlok_sent == 1  or userdata.dbcancel_sent == 1 ) then
                return return_value;
            end
            cancel = true
        end

        if not userdata_is_null and userdata.nonblocking == 1 then
            if cancel and not C.dbdead(client) and userdata.closed == 0  then
                C.dbcancel(client)
                userdata.dbcancel_sent = true
            end

            if userdata.nonblocking_error.is_set == 0 then
                userdata.nonblocking_error.is_message = false
                userdata.nonblocking_error.cancel = cancel
                local error_str = ffi_string(dberrstr)
                ffi_copy(userdata.nonblocking_error.error, error_str, #error_str)

                userdata.nonblocking_error.source = source
                userdata.nonblocking_error.severity = severity
                userdata.nonblocking_error.dberr  = dberr
                userdata.nonblocking_error.oserr  = oserr
                userdata.nonblocking_error.is_set = true
            end
        else
            raise_error(dbproc, 0, cancel, dberrstr, source, severity, dberr, oserr)
        end

        return return_value
    end

--DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line
local function msg_hander(dbproc, msgno, msgstate, severity, msgtext, srvname, procname, line)
    local source = "message"

    local client = ffi_cast("DBPROCESS *", dbproc)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)

    local is_message_an_error = severity > 10 ;
    local userdata_is_null = ffi_cast("void *", userdata) <= nil
    if not userdata_is_null and
       userdata.nonblocking == 1 then
        if userdata.nonblocking_error.is_set == 0 then
            userdata.nonblocking_error.is_message = (not is_message_an_error)
            userdata.nonblocking_error.cancel = is_message_an_error
            local error_str = ffi_string(msgtext)
            ffi_copy(userdata.nonblocking_error.error, error_str, #error_str)
            userdata.nonblocking_error.source = source
            userdata.nonblocking_error.severity = severity
            userdata.nonblocking_error.dberr  = msgno
            userdata.nonblocking_error.oserr  = msgstate
            userdata.nonblocking_error.is_set = true
        end

        if is_message_an_error and not C.dbdead(client) and userdata.closed  == 0 then
            C.dbcancel(client)
            userdata.dbcancel_sent = true
        end
    else
        raise_error(dbproc, not is_message_an_error, is_message_an_error, msgtext, source, severity, msgno, msgstate)
    end

    return 0
end

_M.C = C

function _M.new(self)

    if C.dbinit() == 0 then
        return nil, "Cannot initialize freetds.  Do you have it installed - libfreetds?"
    end

    local userdata = ffi_new("client_userdata");

    userdata.closed     = true;
    userdata.timing_out = false;
    userdata.dbsql_sent = false;
    userdata.dbsqlok_sent   = false;
    userdata.dbcancel_sent  = false;
    userdata.nonblocking    = false;
    userdata.nonblocking_error.is_set = false;
    userdata.message_handler = nil
    local cwrap = {
        closed  = true,
        charset = nil,
        ffi     = ffi,
        is_compact_arrays = false,
        userdata = userdata
    }
    return setmetatable(cwrap, mt)
end

function _M.set_timeout(self, timeout)
    return C.dbsettime(timeout)
end

function _M.set_keepalive(self, max_idle_timeout, pool_size)
end

function _M.get_reused_times(self)
end

local function c_bool_to_str(ret)
    return ret == 1 and "success" or "failure"
end

--[[
mysql = {
    timeout = 120000,
    connect_config = {
        host = "demo.lhdrr.com",
        port = 1433,
        database = "uctoo_lvhuan",
        user = "root",
        password = "LVHUANmysql123",
        max_packet_size = 1024 * 1024,
        charset = "utf8"
    },
    pool_config = {
        max_idle_timeout = 20000, -- 20s
        pool_size = 50 -- connection pool size
    }
}
--]]
function _M.connect(self, opts)

    local cb_err = ffi_cast("EHANDLEFUNC",err_hander)
    self.cb_err = cb_err
    C.dberrhandle(cb_err)

    local cb_msg = ffi_cast("MHANDLEFUNC",msg_hander)
    self.cb_msg = cb_msg
    C.dbmsghandle(cb_msg)

    local login = ffi_new("LOGINREC *")

    login = C.dblogin()
    self.login = login
    if opts.version then
        local version = _t_db_version[opts.version] or 5
        local ret = C.dbsetlversion(login, version)
        ngx_log(ngx_DEBUG, "set version " , c_bool_to_str(ret))
    end

    if opts.login_time then
        local login_time = opts.login_time or 15
        local ret = C.dbsetlogintime(login_time)
        ngx_log(ngx_DEBUG, "set login time " , c_bool_to_str(ret))
    end

    local config = opts.connect_config

    if config.port then
        config.host = sfmt("%s:%d", config.host, config.port)
        config.port = nil
    end

    for k, v in pairs(config) do
        local ret = setup(k, v, login)
        ngx_log(ngx_DEBUG, "set ",k ," ", c_bool_to_str(ret), ", val: ", v)
    end

    local host = config.host
    local client = ffi_new("DBPROCESS *")

    client = C.dbopen(login, host)

    if ffi_cast("void *", client) <= nil  then
        C.dbloginfree(login)
        cb_msg:free()
        cb_err:free()
        self.cb_msg = nil
        self.cb_err = nil
        return nil, "connect failure."
    end

    self.client  = client
    self.closed  = false;
    self.charset = config.charset;

    if opts.version then
        local version = _t_db_version[opts.version] or 5
        local ret = C.dbsetversion(version)
        ngx_log(ngx_DEBUG, "set version " , c_bool_to_str(ret),", val: ", version)
    end

    if opts.timeout then
        local str_timeout = tostring(opts.timeout)
        local res = C.dbsetopt(client, 34, str_timeout, 0)
        ngx_log(ngx_DEBUG, "set timeout opt " , res == 1 and "success" or "failure")
        if res == 0 then
            local ret = C.dbsettime(opts.timeout)
            ngx_log(ngx_DEBUG, "set time " , c_bool_to_str(ret))
        end
    end

    local byte_userdata = ffi_cast("BYTE *", self.userdata)
    C.dbsetuserdata(client, byte_userdata)
    self.userdata.closed = false

    if config.database and not opts.azure then
        local ret = C.dbuse(client, config.database)
        ngx_log(ngx_DEBUG, "dbuse: ",config.database , " ",c_bool_to_str(ret))

    end

    self.encoding = config.charset
    local tds_ver = C.dbtds(client)
    ngx_log(ngx_DEBUG, "get tds version = ", tds_ver)

    if tds_ver <= 7 then
        self.identity_insert_sql =  "SELECT CAST(@@IDENTITY AS bigint) AS Ident";
    else
        self.identity_insert_sql = "SELECT CAST(SCOPE_IDENTITY() AS bigint) AS Ident";
    end

    return self
end

function _M.close(self)

    if self.client and not self.closed then
        C.dbfreebuf(self.client)
        C.dbclose(self.client)
        self.client = nil
        self.closed = true
        self.userdata.closed = true;

        self.cb_msg:free()
        self.cb_err:free()
        self.cb_msg = nil
        self.cb_err = nil
    end

    return true
end

local function require_open_client(self)
    if self.closed or self.userdata.closed == 1 then
        error("closed connection" )
        return false
    end
    return true
end

local function _reset_userdata(userdata)

    userdata.timing_out = false
    userdata.dbsql_sent = false
    userdata.dbsqlok_sent = false
    userdata.dbcancel_sent = false
    userdata.nonblocking = false
    userdata.nonblocking_error.is_set = false

    return userdata
end

local function _dbcancel(client)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)

    C.dbcancel(client)
    userdata.dbcancel_sent = true
end

local function _setup(client)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)
          userdata.nonblocking = true
end

local function _cleanup(client)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)
          userdata.nonblocking = false

    if userdata.nonblocking_error.is_set == 1 then
        userdata.nonblocking_error.is_set = false
        raise_error(client,
            userdata.nonblocking_error.is_message,
            userdata.nonblocking_error.cancel,
            userdata.nonblocking_error.error,
            userdata.nonblocking_error.source,
            userdata.nonblocking_error.severity,
            userdata.nonblocking_error.dberr,
            userdata.nonblocking_error.oserr)
    end
end

local function _dbsqlok(client)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)

    _setup(client)
    local retcode = C.dbsqlok(client)
    _cleanup(client)

    userdata.dbsqlok_sent = true;

    return retcode
end

local function _dbsqlexec(client)
    _setup(client)
    local retcode = C.dbsqlexec(client)
    _cleanup(client)
    return retcode
end

local function _dbresults(client)
    _setup(client)
    local retcode = C.dbresults(client)
    _cleanup(client)
    return retcode
end

local function _dbnextrow(client)
    _setup(client)
    local retcode = C.dbnextrow(client)
    _cleanup(client)
    return retcode
end

_M._dbcancel  = _dbcancel
_M._dbsqlok   = _dbsqlok
_M._dbsqlexec = _dbsqlexec
_M._dbresults = _dbresults
_M._dbnextrow = _dbnextrow


local function db_sql_ok(client)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)

    if userdata.dbsqlok_sent  == 0 then
        userdata.dbsqlok_retcode = _dbsqlok(client)
    end
    return userdata.dbsqlok_retcode
end

local function db_exec(client)
    local dbsqlok_rc = db_sql_ok(client)
    local userdata = ffi_new("client_userdata *")
          userdata = C.dbgetuserdata(client)
          userdata = ffi_cast("client_userdata *",userdata)

    if dbsqlok_rc == 1 then
        while(_dbresults(client) == 1) do
            while( C.dbnextrow(client) ~= -2) do
            end
        end
    end
    C.dbcancel(client)
    userdata.dbcancel_sent = true
    userdata.dbsql_sent = false
end

_M.db_sql_ok= db_sql_ok
_M.db_exec  = db_exec

function _M.send_query(self, query)
    self.userdata = _reset_userdata(self.userdata)
    local client = self.client

    require_open_client(self)

    local ret = C.dbcmd(client, query)
    ngx_log(ngx_DEBUG, "dbcmd() " , c_bool_to_str(ret))


    if C.dbsqlsend(client) == 0 then
        local err = "dbsqlsend() returned FAIL.\n"
        ngx_log(ngx_WARN, err)
        return false, err
    end

    self.userdata.dbsql_sent = true
    return true
end

function _M.read_result(self, opts)

    local result = results:new(self)
    result.encoding = self.encoding

    local rows, fields = result:each(opts)
    ngx_log(ngx_DEBUG, "read data of rows: ", #rows,  ", of fields: ", #fields ,".")
    return rows, fields
end

function _M.query(self, query, opts)

    local send_flag, err = self:send_query(query)

    if not send_flag then
        return nil , err
    end

    return self:read_result(opts)
end

function _M.server_ver(self)
    if self.client then
        return C.dbtds(self.client)
    end

    return nil
end

function _M.set_compact_arrays(flag)
    self.is_compact_arrays = flag
    return true
end

function _M.encoding(self)
    return self.encoding
end

function _M.charset(self)
    return self.charset
end

function _M.sqlsent(self)
    return self.userdata.dbsql_sent
end

function _M.canceled(self)
    return self.userdata.dbcancel_sent
end

function _M.closed(self)
    return self.closed or self.userdata.closed == 1
end

function _M.dead(self)
    return C.dbdead(self.client) == 1 and true or false;
end

return _M