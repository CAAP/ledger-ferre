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
local remove	  = table.remove

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
	   	   facturas = 'tienda, uid, fapi PRIMARY KEY NOT NULL, rfc NOT NULL, sat NOT NULL',
		   updates = 'vers INTEGER PRIMARY KEY, clave, msg'} -- just ONE ledger & ups record

local QID	 = 'SELECT * FROM datos WHERE clave LIKE %s'
local QVERS	 = 'SELECT MAX(vers) vers FROM updates'
local QTKTS	 = 'SELECT tienda, MAX(uid) uid FROM tickets GROUP BY tienda'
local UVERS	 = 'SELECT * FROM datos WHERE clave IN (SELECT DISTINCT(clave) FROM updates WHERE vers > %d)'

local ISSTR	 = {desc=true, fecha=true, obs=true, proveedor=true, gps=true, u1=true, u2=true, u3=true, uidPROV=true}
local TOLL	 = {costo=true, impuesto=true, descuento=true, rebaja=true}
local DIRTY	 = {clave=true, tbname=true, fruit=true}
local PRCS	 = {prc1=true, prc2=true, prc3=true}
local UPQ	 = 'UPDATE %q SET %s %s'
local COSTOL 	 = 'costol = costo*(100+impuesto)*(100-descuento)*(1-rebaja/100.0)'

local UID	 = {}
local DB	 = {}

local INDEX

local secret = "hjLXIbvtt/N57Ara]e!@gHF=}*n&g$odQVsNG^jb"

-- Local function definitions --
--------------------------------
--
local function maxV() return fd.first(DB[WEEK].query(QVERS), function(x) return x end).vers  end

local function plain(a) return fromJSON(a) end

local function smart(v, k) return ISSTR[k] and format("'%s'", tostring(v):upper()) or (tointeger(v) or tonumber(v) or 0) end

local function found(a, b) return fd.first(fd.keys(a), function(_,k) return b[k] end) end

local function sanitize(b) return function(_,k) return not(b[k]) end end

local function reformat(v, k)
    local vv = smart(v, k)
    return format('%s = %s', k, vv)
end

local function indexar(w)
    return function(a)
	return fd.reduce(INDEX, fd.map(function(k) return a[k] or '' end), fd.into, w)
    end
end

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
    local vv = maxV()
    local vers = w.vers
    local uid = UID[id]
    local ret = {}

    if vers > vv then
	ret[#ret+1] = {id, 'adjust', 'vers', vv}
    elseif vers < vv then
--	updates('vers', id, vers, ret)
    end

    if w.uid > uid then
	ret[#ret+1] = {id, 'adjust', 'uid', uid}
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

local function addUp(w)
    local conn = DB.ferre
    local clause = format('WHERE clave LIKE %s', w.clave)
    local toll = found(w, TOLL)

    local u = fd.reduce(fd.keys(w), fd.filter(sanitize(DIRTY)), fd.map(reformat), fd.into, {})
    if #u == 0 then return '' end
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

    return w
end

local function addAnUpdate(conn, u)
    return function(s, j)
	local o = fromJSON(s)
	o.costol = nil

	local clave  = tointeger(o.clave) or format('%q', o.clave)
	local a = fd.first(conn.query(format(QID, clave)), function(x) return x end)
	local b = {clave=o.clave}; for k,v in pairs(o) do if a[k] ~= v then b[k] = v end end
	local q = format("INSERT INTO updates VALUES (%d, %s, '%s')", j+u, clave, addUp(b))

	-- either an update was stored or already in place, update vers
	assert( DB[WEEK].exec( q ) )
	print('clave:', clave, '\n')
    end
end

local function addTickets(id, msg)
    local conn = DB[WEEK]

    if #msg > 8 then
	fd.slice(5, msg, fd.map(plain), fd.map(indexar{id}), into'tickets', conn)
    else
	fd.reduce(msg, fd.map(plain), fd.map(indexar{id}), into'tickets', conn)
    end

    UID[id] = fromJSON(msg[#msg]).uid
    return format('UID:\t%s', w.uid)
end

local function addUpdates(id, msg)
    local conn = DB.ferre
    local u = remove(msg, 1)
    fd.reduce(msg, addAnUpdate(conn, u-#msg))
    return format('vers:\t%d', u)
end

local function process(id, msg)
    local cmd = remove(msg, 1)

    if cmd == 'ticket' then
	return addTickets(id, msg)

    elseif cmd == 'update' then
	return addUpdates(id, msg)

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
print('updates:', maxV(), 'tickets:', conn.count'tickets', '\n')

fd.reduce(fd.keys(SKS), function(_,s) UID[s] = '0' end)
fd.reduce(conn.query( QTKTS ), function(a) UID[a.tienda] = a.uid end)

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

	    print(id, concat(msg, '\t'), '\n')

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

