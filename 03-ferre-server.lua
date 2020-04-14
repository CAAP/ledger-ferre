#! /usr/bin/env lua53

-- Import Section
--
local fd	  = require'carlos.fold'

local receive	  = require'carlos.ferre'.receive
local send	  = require'carlos.ferre'.send
local asweek	  = require'carlos.ferre'.asweek
local dbconn	  = require'carlos.ferre'.dbconn
local newTable	  = require'carlos.sqlite'.newTable
local pollin	  = require'lzmq'.pollin
local context	  = require'lzmq'.context
local asJSON	  = require'json'.encode
local fromJSON	  = require'json'.decode

local format	  = require'string'.format
local concat	  = table.concat
local assert	  = assert
local print	  = print

local WEEK	  = asweek( os.time() )

-- No more external access after this point
_ENV = nil -- or M

-- Local Variables for module-only access
--
local DOWNSTREAM = 'ipc://downstream.ipc' --  

local UPDATES	 = 'tcp://*:5610'
local SKS	 = {["FA-BJ-01"]=true}

local TABS	 = {tickets = 'tienda, uid, tag, prc, clave, desc, costol NUMBER, unidad, precio NUMBER, unitario NUMBER, qty INTEGER, rea INTEGER, totalCents INTEGER, uidSAT, nombre',
		   updates = 'tienda, vers INTEGER PRIMARY KEY, clave, msg',
	   	   facturas = 'tienda, uid, fapi PRIMARY KEY NOT NULL, rfc NOT NULL, sat NOT NULL'}

local QVERS	 = 'SELECT tienda, MAX(vers) vers FROM updates GROUP BY tienda'
local QTKTS	 = 'SELECT tienda, MAX(uid) uid FROM tickets GROUP BY tienda'
local UVERS	 = 'SELECT * FROM datos WHERE clave IN (SELECT DISTINCT(clave) FROM updates WHERE vers > %d)'

local CACHE	 = {}
local DB	 = {}

local secret = "hjLXIbvtt/N57Ara]e!@gHF=}*n&g$odQVsNG^jb"

-- Local function definitions --
--------------------------------
--

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

---------------------------------
-- Program execution statement --
---------------------------------

-- Initialize databases
local conn = assert( dbconn'ferre' )
DB.ferre = conn

conn = assert( dbconn(WEEK, true) )
fd.reduce(fd.keys(TABS), function(schema, tbname) connexec(WEEK, format(newTable, tbname, schema)) end)
DB[WEEK] = conn

print("ferre & week DBs were successfully open\n")
print('updates:', conn.count'updates', 'tickets:', conn.count'tickets', '\n')

fd.reduce(fd.keys(SKS), function(_,s) CACHE[s] = {} end)
fd.reduce(conn.query( QVERS ), function(a) local w = CACHE[a.tienda]; w.vers = a.vers end)
fd.reduce(conn.query( QTKTS ), function(a) local w = CACHE[a.tienda]; w.uid = a.uid end)

-- Initialize servers
local CTX = context()

local ups = assert(CTX:socket'ROUTER')

assert( ups:curve( secret ) )

assert( ups:bind( UPDATES ) )

print('\nSuccessfully bound to:', UPDATES, '\n')

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
		    ups:send_msgs( switch(id, w) )
		end
	    end

	end

    end

end

