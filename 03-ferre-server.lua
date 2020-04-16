#! /usr/bin/env lua53

-- Import Section
--
local fd	  = require'carlos.fold'

local receive	  = require'carlos.ferre'.receive
local send	  = require'carlos.ferre'.send
local asweek	  = require'carlos.ferre'.asweek
local dbconn	  = require'carlos.ferre'.dbconn
local newTable	  = require'carlos.sqlite'.newTable
local into	  = require'carlos.sqlite'.into
local pollin	  = require'lzmq'.pollin
local context	  = require'lzmq'.context
local asJSON	  = require'json'.encode
local fromJSON	  = require'json'.decode

local tonumber  = tonumber
local tostring	= tostring
local tointeger = math.tointeger

local format	  = string.format
local concat	  = table.concat

local pairs	  = pairs
local assert	  = assert
local print	  = print

local WEEK	  = asweek( os.time() )

-- No more external access after this point
_ENV = nil -- or M

-- Local Variables for module-only access
--
local DOWNSTREAM = 'ipc://downstream.ipc'

local UPDATES	 = 'tcp://*:5610'
local SKS	 = {["FA-BJ-01"]=true}

local TABS	 = {tickets = 'tienda, uid, tag, prc, clave, desc, costol NUMBER, unidad, precio NUMBER, unitario NUMBER, qty INTEGER, rea INTEGER, totalCents INTEGER, uidSAT, nombre',
		   updates = 'tienda, vers INTEGER PRIMARY KEY, clave, msg',
	   	   facturas = 'tienda, uid, fapi PRIMARY KEY NOT NULL, rfc NOT NULL, sat NOT NULL'}

local QID	 = 'SELECT * FROM datos WHERE clave LIKE %s'
local QVERS	 = 'SELECT tienda, MAX(vers) vers FROM updates GROUP BY tienda'
local QTKTS	 = 'SELECT tienda, MAX(uid) uid FROM tickets GROUP BY tienda'
local UVERS	 = 'SELECT * FROM datos WHERE clave IN (SELECT DISTINCT(clave) FROM updates WHERE vers > %d)'

local ISSTR	 = {desc=true, fecha=true, obs=true, proveedor=true, gps=true, u1=true, u2=true, u3=true, uidPROV=true}
local TOLL	 = {costo=true, impuesto=true, descuento=true, rebaja=true}
local DIRTY	 = {clave=true, tbname=true, fruit=true}
local PRCS	 = {prc1=true, prc2=true, prc3=true}
local UPQ	 = 'UPDATE %q SET %s %s'
local COSTOL 	 = 'costol = costo*(100+impuesto)*(100-descuento)*(1-rebaja/100.0)'

local CACHE	 = {}
local DB	 = {}

local TICKETS	 = {ticket=true, presupuesto=true}

local INDEX

local secret = "hjLXIbvtt/N57Ara]e!@gHF=}*n&g$odQVsNG^jb"

-- Local function definitions --
--------------------------------
--
local function smart(v, k) return ISSTR[k] and format("'%s'", tostring(v):upper()) or (tointeger(v) or tonumber(v) or 0) end

local function reformat(v, k)
    local vv = smart(v, k)
    return format('%s = %s', k, vv)
end

local function found(a, b) return fd.first(fd.keys(a), function(_,k) return b[k] end) end

local function sanitize(b) return function(_,k) return not(b[k]) end end

local function indexar(a) return fd.reduce(INDEX, fd.map(function(k) return a[k] or '' end), fd.into, {}) end

local function updates(cmd, id, old, ret)
    local function wired(s) return {id, 'update', s} end
    local conn = DB[WEEK]

    if cmd == 'vers' then
	local q = format(UVERS, old)
	fd.reduce(conn.query(q), fd.map(asJSON), fd.map(wired), fd.into, ret)
	return ret
    end
end

local function switch(id, w)
    local y = CACHE[id]
    local vers = w.vers
    local uid = w.uid
    local ret = {}

    if vers > y.vers then
	ret[#ret+1] = {id, 'adjust', 'vers', y.vers}
    elseif vers < y.vers then
	updates('vers', id, vers, ret)
    end

    if uid > y.uid then
	ret[#ret+1] = {id, 'adjust', 'uid', y.uid}
    end

    ret[#ret+1] = {id, 'OK'}

    return ret
end

local function up_costos(w, a) -- conn
    for k in pairs(TOLL) do w[k] = nil end
    fd.reduce(fd.keys(a), fd.filter(function(_,k) return k:match'^precio' or k:match'^costo' end), fd.merge, w)
    return w
end

local function up_precios(conn, w, clause)
    local qry = format('SELECT * FROM precios %s LIMIT 1', clause)
    local a = fd.first(conn.query(qry), function(x) return x end)

    fd.reduce(fd.keys(w), fd.filter(function(_,k) return k:match'prc' end), fd.map(function(_,k) return k:gsub('prc', 'precio') end), fd.rejig(function(k) return a[k], k end), fd.merge, w)

    for k in pairs(PRCS) do w[k] = nil end

    return a, w
end

local function addUp(clave, w)
    local conn = DB.ferre
    local clause = format('WHERE clave LIKE %s', clave)
    local toll = found(w, TOLL)

    local u = fd.reduce(fd.keys(w), fd.filter(sanitize(DIRTY)), fd.map(reformat), fd.into, {})
    if #u == 0 then return false end -- safeguard
    local qry = format(UPQ, 'datos', concat(u, ', '), clause)

    assert( conn.exec( qry ) )
    if toll then
	qry = format(UPQ, 'datos', COSTOL, clause)
	assert( conn.exec( qry ) )
    end

    if found(w, PRCS) or toll then
	local a = up_precios(conn, w, clause)
	if toll then up_costos(w, a) end
    end

end

local function addAnUpdate(id, msg)
    local w = CACHE[id]
    local conn = DB.ferre
    local o = fromJSON(msg[2])
    o.costol = nil; o.tag = nil;
    local clave  = tointeger(o.clave) or format('%q', o.clave)

    local a = fd.first(conn.query(format(QID, clave)), function(x) return x end)
    local b = {}; for k,v in pairs(o) do if a[k] ~= v then b[k] = v end end
    addUp(clave, b)

    conn = DB[WEEK]
    b.clave = o.clave
    local u = w.vers+1
    local q = format("INSERT INTO updates VALUES (%q, %d, %s, '%s')", id, u, clave, asJSON(b))
    assert( conn.exec( q ) )

    w.vers = u
    return format('vers:\t%d\n', u)
end

local function addTicket(id, msg)
    local w = CACHE[id]
    local conn = DB[WEEK]
    local q = fromJSON(msg[2])
    q.tienda = id

    fd.reduce({q}, fd.map(indexar), into'tickets', conn)

    w.uid = q.uid
    return format('UID:\t%s\n', q.uid)
end

local function process(id, msg)
    local cmd = msg[1]

    if TICKETS[cmd] then
	return addTicket(id, msg)
    elseif cmd == 'update' then
	return addAnUpdate(id, msg)
    end

end

---------------------------------
-- Program execution statement --
---------------------------------

-- Initialize databases
local conn = assert( dbconn'ferre' )
DB.ferre = conn

conn = assert( dbconn(WEEK, true) )
fd.reduce(fd.keys(TABS), function(schema, tbname) conn.exec(format(newTable, tbname, schema)) end)
DB[WEEK] = conn

print("ferre & week DBs were successfully open\n")
print('updates:', conn.count'updates', 'tickets:', conn.count'tickets', '\n')

fd.reduce(fd.keys(SKS), function(_,s) CACHE[s] = {vers=0, uid='0'} end)
fd.reduce(conn.query( QVERS ), function(a) local w = CACHE[a.tienda]; w.vers = a.vers end)
fd.reduce(conn.query( QTKTS ), function(a) local w = CACHE[a.tienda]; w.uid = a.uid end)

INDEX = conn.header'tickets'

-- Initialize servers
local CTX = context()

local ups = assert(CTX:socket'ROUTER')

assert( ups:curve( secret ) )

assert( ups:bind( UPDATES ) )

print('\nSuccessfully bound to:', UPDATES, '\n')

local function receive(srv)
    local id, more = srv:recv_msg(true)
    local ms = fd.reduce(function() return srv:recv_msgs(true) end, fd.into, {})
    return id, ms
end

--
while true do

    print'+\n'

    if pollin{ups} then -- msgs, spy, tasks

	if ups:events() == 'POLLIN' then
	    local id, msg = receive( ups )
	    local cmd = msg[1]:match'%a+'

	    print(id, concat(msg, '\t'))

	    if SKS[id] then
		if cmd == 'Hi' then
		    local w = fromJSON(msg[2])
		    local q = switch(id, w)
		    fd.reduce(q, function(a) ups:send_msgs(a) end)

		else -- if TICKETS[cmd] then
		    print(process( id, msg ), '\n')

		end

	    end

	end

    end

end

