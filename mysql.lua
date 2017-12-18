--[[
	@author: codesun
	@email:  cscodesun@gmail.com
	@data:   2017/11/11
	@description: native mysql client for Lua base on blocking/nonblocking driver
--]]

driver = require "mysock"

local strbyte, strchar, strsub = string.byte, string.char, string.sub
local lshift, rshift = bit.lshift, bit.rshift
local band, bor, bxor = bit.band, bit.bor, bit.bxor
local concat = table.concat

local RET_DONE  = 0
local RET_MORE  = 1
local RET_AGAIN = 2

-- Refers mysql/my_command.h
local Command = {
	SLEEP               = 0x00,
	QUIT                = 0x01,
	QUERY               = 0x03,
	FIELD_LIST          = 0x04,
	CREATE_DB           = 0x05,
	DROP_DB             = 0x06,
	REFRESH             = 0x07,
	SHUTDOWN            = 0x08,
	STATISTICS          = 0x09,
	PROCESS_INFO        = 0x0A,
	CONNECT             = 0x0B,
	PROCESS_KILL        = 0x0C,
	DEBUG               = 0x0D,
	PING                = 0x0E,
	TIME                = 0x0F,
	DELAYED_INSERT      = 0x10,
	CHANGE_USER         = 0x11,
	BINLOG_DUMP         = 0x12,
	TABLE_DUMP          = 0x13,
	CONNECT_OUT         = 0x14,
	REGISTER_SLAVE      = 0x15,
	STMT_PREPARE        = 0x15,
	STMT_EXECUTE        = 0x17,
	STMT_SEND_LONG_DATA = 0x18,
	STMT_CLOSE          = 0x19,
	STMT_RESET          = 0x1A,
	SET_OPTION          = 0x1B,
	STMT_FETCH          = 0x1C,
	DAEMON              = 0x1D,
	BINLOG_DUMP_GTID    = 0x1E,
	RESET_CONNECTION    = 0x1F,
}

-- Refers mysql/mysql_com.h
local ClientFlag = {
	LONG_PASSWORD                  = 0x00000001, -- Uew more secure passwords
	FOUND_ROWS                     = 0x00000002, -- Found instead of affected rows
	LONG_FLAG                      = 0x00000004, -- Get all column flags
	CONNECT_WITH_DB                = 0x00000008, -- One can specify db on connect
	NO_SCHEMA                      = 0x00000010, -- Don't allow database.table.column
	COMPRESS                       = 0x00000020, -- Can use compression protocol
	ODBC                           = 0x00000040, -- Odbc client
	LOCAL_FILES                    = 0x00000080, -- Can use LOAD DATA LOCAL
	IGNORE_SPACE                   = 0x00000100, -- Ignore spaces before '('
	PROTOCOL_41                    = 0x00000200, -- New 4.1 protocol
	INTERACTIVE                    = 0x00000400, -- This is an interactive client
	SSL                            = 0x00000800, -- Switch to SSL after handshake
	IGNORE_SIGPIPE                 = 0x00001000, -- Ignore sigpipes
	TRANSACTIONS                   = 0x00002000, -- Client knows about transactions
	RESERVED                       = 0x00004000, -- Old flag for 4.1 protocol
	SECURE_CONNECTION              = 0x00008000, -- New 4.1 authentication
	MULTI_STATEMENTS               = 0x00010000, -- Enable/disable multi-stmt support
	MULTI_RESULTS                  = 0x00020000, -- Enable/disable multi-results
	PS_MULTI_RESULTS               = 0x00040000, -- Multi-results in PS-protocol
	PLUGIN_AUTH                    = 0x00080000, -- Client supports plugin authentication
	CONNECT_ATTRS                  = 0x00100000, -- Client supports connection attributes
	PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000, -- Enable authentication response packet to be larger than 255 bytes
	CAN_HANDLE_EXPIRED_PASSWORDS   = 0x00400000, -- Don't close the connection for a connection with expired password
	SESSION_TRACK                  = 0x00800000, -- Capable of handling server state change information
	                                             -- A hint to the server to include the state change infomation in OK packet
	DEPRECATE_EOF                  = 0x01000000, -- Client no longer needs EOF packet
	SSL_VERIFY_SERVER_CERT         = 0x40000000,
	REMEMBER_OPTIONS               = 0x80000000,
}

-- Refers mysql/mysql_com.h
local ServerFlag = {
	STATUS_IN_TRANS                    = 0x0001,
	STATUS_AUTOCOMMIT                  = 0x0002,
	SERVER_MORE_RESULTS_EXISTS         = 0x0008,
	SERVER_QUERY_NO_GOOD_INDEX_USED    = 0x0010,
	SERVER_QUERY_NO_INDEX_USED         = 0x0020,
	STATUS_CURSOR_EXISTS               = 0x0040,
	SERVER_STATUS_LAST_ROW_SEND        = 0x0080,
	SERVER_STATUS_DB_DROPPED           = 0x0100,
	SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200,
	SERVER_STATUS_METADATA_CHANGED     = 0x0400,
	SERVER_QUERY_WAS_SLOW              = 0x0800,
	SERVER_PS_OUT_PARAMS               = 0x1000,
	SERVER_STATUS_IN_TRANS_READONLY    = 0x2000,
	SERVER_SESSION_STATE_CHANGED       = 0x4000,
}

-- Refers mysql/mysql_com.h
FieldFlag = {
	NOT_NULL           = 0x000001,
	PRI_KEY            = 0x000002,
	UNIQUE_KEY         = 0x000004,
	MULTIPLE_KEY       = 0x000008,
	BLOB               = 0x000010,
	UNSIGNED           = 0x000020,
	ZEROFILL           = 0x000040,
	BINARY             = 0x000080,
	ENUM               = 0x000100,
	AUTO_INCREMENT     = 0x000200,
	TIMESTAMP          = 0x000400,
	SET                = 0x000800,
	NO_DEFAULT_VALUE   = 0x001000,
	ON_UPDATE_VALUE    = 0x002000,
	PART_KEY           = 0x004000,
	NUM                = 0x008000,
	GROUP              = 0x008000,
	UNIQUE             = 0x010000,
	BINCMP             = 0x020000,
	GET_FIXED_FIELDS   = 0x040000,
	FIELD_IN_PART_FUNC = 0x080000,
	FIELD_IN_ADD_INDEX = 0x100000,
	FIELD_IS_RENAMED   = 0x200000,
}

-- Refers mysql/mysql_com.h
FieldType = {
    DECIMAL     = 0x00,
    TINY        = 0x01,
    SHORT       = 0x02,
    LONG        = 0x03,
    FLOAT       = 0x04,
    DOUBLE      = 0x05,
    NULL        = 0x06,
    TIMESTAMP   = 0x07,
    LONGLONG    = 0x08,
    INT24       = 0x09,
    DATE        = 0x0a,
    TIME        = 0x0b,
    DATETIME    = 0x0c,
    YEAR        = 0x0d,
    NEWDATE     = 0x0e,
    VARCHAR     = 0x0f,
    BIT         = 0x10,
    NEWDECIMAL  = 0xf6,
    ENUM        = 0xf7,
    SET         = 0xf8,
    TINY_BLOB   = 0xf9,
    MEDIUM_BLOB = 0xfa,
    LONG_BLOB   = 0xfb,
    BLOB        = 0xfc,
    VAR_STRING  = 0xfd,
    STRING      = 0xfe,
    GEOMETRY    = 0xff,
}

Charset = {
    big5      = 1,
    dec8      = 3,
    cp850     = 4,
    hp8       = 6,
    koi8r     = 7,
    latin1    = 8,
    latin2    = 9,
    swe7      = 10,
    ascii     = 11,
    ujis      = 12,
    sjis      = 13,
    hebrew    = 16,
    tis620    = 18,
    euckr     = 19,
    koi8u     = 22,
    gb2312    = 24,
    greek     = 25,
    cp1250    = 26,
    gbk       = 28,
    latin5    = 30,
    armscii8  = 32,
    utf8      = 33,
    ucs2      = 35,
    cp866     = 36,
    keybcs2   = 37,
    macce     = 38,
    macroman  = 39,
    cp852     = 40,
    latin7    = 41,
    utf8mb4   = 45,
    cp1251    = 51,
    utf16     = 54,
    utf16le   = 56,
    cp1256    = 57,
    cp1257    = 59,
    utf32     = 60,
    binary    = 63,
    geostd8   = 92,
    cp932     = 95,
    eucjpms   = 97,
    gb18030   = 248
}

local Res = {
	OK  = 0x00,
	EOF = 0xFE,
	ERR = 0xFF,
}

------------------------------------------------------------------------
--                         Internal Functions                         --
------------------------------------------------------------------------

local __GetToken = driver.getToken
local __recv = driver.recv
local __send = driver.send
local __connect = driver.connect
local __reset = driver.reset
local __close = driver.close

local function __RecvPacket(drv)
	local res = {}
	local ok, data
	repeat
		ok, data = __recv(drv)
		if ok ~= nil then
			res[#res + 1] = data
		end
	until ok
	return true, concat(res)
end

local function __SendPacket(drv, data)
	local ok
	repeat
		ok = __send(drv, data)
	until ok
	return true
end

local function __Connect(drv, addr, port)
	local ok
	repeat
		ok = __connect(drv, addr, port)
	until ok
	return true
end

local __CastMap = {
	[FieldType.DECIMAL]    = tonumber,
	[FieldType.TINY]       = tonumber,
	[FieldType.SHORT]      = tonumber,
	[FieldType.LONG]       = tonumber,
	[FieldType.FLOAT]      = tonumber,
	[FieldType.DOUBLE]     = tonumber,
	[FieldType.INT24]      = tonumber,
	[FieldType.YEAR]       = tonumber,
	[FieldType.NEWDECIMAL] = tonumber,
}

local function __GetCString(buf, pos)
	local last = string.find(buf, "\0", pos, true)
	return strsub(buf, pos, last - 1), last + 1
end

local function __GetStringN(buf, pos, n)
	return strsub(buf, pos, pos + n - 1), pos + n
end

local function __GetInt2(buf, pos)
	local a, b = strbyte(buf, pos, pos + 1)
	return bor(a, lshift(b, 8)), pos + 2
end

local function __SetInt2(n)
	return strchar(band(n, 0xFF), band(rshift(n, 8), 0xFF))
end

local function __GetInt3(buf, pos)
	local a, b, c = strbyte(buf, pos, pos + 2)
	return bor(a, lshift(b, 8), lshift(c, 16)), pos + 3
end

local function __SetInt3(n)
	return strchar(band(n, 0xFF), band(rshift(n, 8), 0xFF), band(rshift(n, 16), 0xFF))
end

local function __GetInt4(buf, pos)
	local a, b, c, d = strbyte(buf, pos, pos + 3)
	return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24)), pos + 4
end

local function __SetInt4(n)
	return strchar(band(n, 0xFF), band(rshift(n, 8), 0xFF),
		band(rshift(n, 16), 0xFF), band(rshift(n, 24), 0xFF))
end

local function __GetInt8(buf, pos)
	local lo, hi = 0, 0
	lo, pos = __GetInt4(buf, pos)
	hi, pos = __GetInt4(buf, pos)
	return lo + hi * 4294967296, pos
end

local function __GetLencInt(buf, pos)
	local prefix
	prefix, pos = strbyte(buf, pos), pos + 1
	if prefix >= 0 and prefix < 0xFB then
		return prefix, pos
	elseif prefix == 0xFB then
		return nil, pos
	elseif prefix == 0xFC then
		return __GetInt2(buf, pos)
	elseif prefix == 0xFD then
		return __GetInt3(buf, pos)
	elseif prefix == 0xFE then
		return __GetInt8(buf, pos)
	end
end

local function __GetLencString(buf, pos)
	local len
	len, pos = __GetLencInt(buf, pos)
	if len == nil then
		return nil, pos
	end
	return __GetStringN(buf, pos, len)
end

------------------------------------------------------------------------
--                            Field Class                             --
------------------------------------------------------------------------

Field = {}

function Field:new()
	local res = {}
	setmetatable(res, {__index = Field})
	return res
end

function Field:isPrimaryKey()
	return band(self.flag, FieldFlag.PRI_KEY) ~= 0
end


------------------------------------------------------------------------
--                           Packet Parser                            --
------------------------------------------------------------------------

local function __ParseOkPacket(buf)
	local affectedRows = 0
	local lastInsertId = 0
	local serverStatus = 0
	local warningCount = 0
	local info = nil

	local pos = 2
	affectedRows, pos = __GetLencInt(buf, pos)
	lastInsertId, pos = __GetLencInt(buf, pos)
	serverStatus, pos = __GetInt2(buf, pos)
	warningCount, pos = __GetInt2(buf, pos)

	if pos <= #buf then
		info = strsub(buf, pos, #buf)
	end

	return affectedRows, lastInsertId, serverStatus, warningCount, info
end

local function __ParseErrPacket(buf, pos)
	local errCode, pos = __GetInt2(buf, 2)

	local sqlState
	if strsub(buf, pos, pos) == "#" then
		sqlState, pos = __GetStringN(buf, pos + 1, 5)
	end

	local errMsg = strsub(buf, pos)
	return errCode, errMsg, sqlState
end

-- EOF_Packet
--   int<1> 0xFE - EOF header
--   int<2> warning count
--   int<2> server status
local function __ParseEofPacket(buf)
	local warningCount = 0
	local serverStatus = 0

	local pos = 2
	warningCount, pos = __GetInt2(buf, pos)
	serverStatus, pos = __GetInt2(buf, pos)
	return warningCount, serverStatus
end

local function __ParseField(buf)
	local pos = 1

	local res = Field:new()
	_, pos = __GetLencString(buf, pos) -- catalog
	_, pos = __GetLencString(buf, pos) -- db
	_, pos = __GetLencString(buf, pos) -- table
	_, pos = __GetLencString(buf, pos) -- org_table
	res.name, pos = __GetLencString(buf, pos) -- name
	_, pos = __GetLencString(buf, pos) -- org_name

	pos = pos + 1 + 2 + 4
	-- 1B filler
	-- 2B charset
	-- 4B length
	res.type, pos = strbyte(buf, pos), pos + 1
	res.flag, pos = __GetInt2(buf, pos)

	-- 1B precision
	-- 2B filler
	-- LENC default value
	return res
end

------------------------------------------------------------------------
--                          ResultSet Class                           --
------------------------------------------------------------------------

ResultSet = {}

function ResultSet:new()
	local res = {}
	setmetatable(res, {__index = ResultSet})
	return res
end

function ResultSet:getRow()
end

function ResultSet:hasMore()
end

function ResultSet:getRows()
	if self.rows == nil then
		local ok, data
		local fields = self.fields
		local rows = {}
		while true do
			ok, data = __RecvPacket(self.__session__.__driver__)
			if not ok then
				return false
			end

			if strbyte(data, 1) == Res.EOF then
				__ParseEofPacket(data)
				break
			end

			local pos = 1
			local content
			local row = {}
			-- construct next row
			for i = 1, #fields do
				content, pos = __GetLencString(data, pos)
				local caster = __CastMap[fields[i].type]
				row[i] = caster and caster(content) or content
			end
			rows[#rows + 1] = row
		end
		self.rows = rows
	end
	return true, self.rows
end

------------------------------------------------------------------------
--                             MySQL API                              --
------------------------------------------------------------------------

MySQL = { __version__ = "0.1.0" }

function MySQL.newConnection(opts)
	-- create new connection instance
	local conn = { __driver__ = driver.new() }
	setmetatable(conn, {__index = MySQL})

	-- Connect DB
	local drv = conn.__driver__
	local host = opts.host or "127.0.0.1"
	local port = opts.port or 3306
	local charset = opts.charset or Charset.utf8
	local maxSize = opts.maxPacketSize or 16 * 1024 * 1024
	local database = opts.database or ""
	local user = opts.user or ""
	local passwd = opts.password or ""

	local ok, data = true, nil

	-- socket connect to the target server
	ok = __Connect(drv, host, port)
	if not ok then
		return false
	end

	-- receive handshake packet from server
	ok, data = __RecvPacket(drv)
	if not ok then
		return false
	end

	local pos = 1
	local authData = nil
	local capabilities = nil

	-- process of handshake packet from server
	-- protocol version <1>
	conn.protocolVersion, pos = strbyte(data, pos), pos + 1
	-- server version <NUL>
	conn.serverVersion, pos = __GetCString(data, pos)
	-- connection id <4>
	conn.connectionId, pos = __GetInt4(data, pos)
	-- scramble 1st part <8>
	authData, pos = __GetStringN(data, pos, 8)
	-- reserved byte <1>
	pos = pos + 1
	-- server capabilities 1st <2>
	capabilities, pos = __GetInt2(data, pos)
	-- character collation <1>
	conn.charset, pos = strbyte(data, pos), pos + 1
	-- status flags <2>
	conn.serverStatus, pos = __GetInt2(data, pos)
	-- server capabilities 2nd <2>
	local capabilitiesSib = nil
	capabilitiesSib, pos = __GetInt2(data, pos)
	capabilities = bor(capabilities, lshift(capabilitiesSib, 16))
	-- length of scramble <1>, usually be 21
	local authDataLen = nil
	authDataLen, pos = strbyte(data, pos), pos + 1
	-- filler <10>
	pos = pos + 10

	if bit.band(capabilities, ClientFlag.SECURE_CONNECTION) then
		local authData2Len = math.min(12, authDataLen - 8)
		local authData2 = nil
		authData2, pos = __GetStringN(data, pos, authData2Len)
		pos = pos + 1 -- skip reversed byte 0x00
		authData = authData .. authData2
	end

	if bit.band(capabilities, ClientFlag.PLUGIN_AUTH) then
		conn.authPlugin, pos = __GetCString(data, pos)
	end

	-- build handshake response
	local token = __GetToken(drv, passwd, authData)
	local clientFlag = 0x3F7CF
	local response = __SetInt4(clientFlag) ..
	                 __SetInt4(maxSize) ..
					 string.char(charset) ..
					 string.rep("\0", 23) ..
					 user .. "\0" ..
					 string.char(#token) .. token..
					 database .. "\0"

	ok = __SendPacket(drv, response)
	if not ok then
		return false
	end

	ok, data = __RecvPacket(drv)
	if not ok then
		return false
	end

	local res = strbyte(data, 1)
	if res == Res.OK then
		--print(__ParseOkPacket(data))
	elseif res == Res.ERR then
		return false
	end

	return true, conn
end

function MySQL:close()
	__reset(self.__driver__)

	local ok = __SendPacket(self.__driver__, strchar(Command.QUIT))
	if not ok then
		return false
	end
	__close(self.__driver__)
	return true
end

function MySQL:exec(sql)
end

function MySQL:query(sql)
	__reset(self.__driver__)
	local ok, data = true, nil
	ok = __SendPacket(self.__driver__, strchar(Command.QUERY) .. sql)
	if not ok then
		return false
	end

	ok, data = __RecvPacket(self.__driver__)
	if not ok then
		return false
	end

	local ret = strbyte(data, 1)

	if ret >= 0 and ret < 0xFB then
		-- parse metadata
		local count = __GetLencInt(data, 1)

		local resSet = ResultSet:new()

		local fields = {}

		for i = 1, count do
			ok, data = __RecvPacket(self.__driver__)
			if not ok then
				return false
			end
			fields[#fields + 1] = __ParseField(data)
		end

		resSet.fields = fields
		resSet.__session__ = self

		-- parse EOF packet
		ok, data = __RecvPacket(self.__driver__)
		if not ok then
			return false
		end
		return true, resSet
	elseif ret == Res.OK then
		__ParseOkPacket(data, 1)
		return true
	elseif ret == Res.ERR then
		__ParseErrPacket(data, 1)
		return false
	end
	--TODO
end

function MySQL:ping()
	__reset(self.__driver__)
	local ok, data = true, nil
	ok = __SendPacket(self.__driver__, strchar(Command.PING))
	if not ok then
		return false
	end

	ok, data = __RecvPacket(self.__driver__)
	if not ok then
		return false
	end

	local res = strbyte(data, 1)
	if res == Res.OK then
		--print("PONG")
		return true
	end
	return false
end

------------------------------------------------------------------------
--                             Test Case                              --
------------------------------------------------------------------------

print("Version:", MySQL.__version__)

ok, conn = MySQL.newConnection({database = "stu", user = "root", password = "root"})
if ok then
	ok, res = conn:query("select * from stu")
	if ok then
		local ok, rows = res:getRows()
		for _, v in ipairs(rows) do
			print(unpack(v))
		end
	end
	conn:close()
end
