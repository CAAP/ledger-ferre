#! /usr/bin/env lua53

-- Import Section
--
local fd	  = require'carlos.fold'

local receive	  = require'carlos.ferre'.receive
local send	  = require'carlos.ferre'.send
local pollin	  = require'lzmq'.pollin
local context	  = require'lzmq'.context
local keypair	  = require'lzmq'.keypair

local format	  = require'string'.format
local concat	  = table.concat
local assert	  = assert
local print	  = print
local pairs	  = pairs
local toint	  = math.tointeger

-- No more external access after this point
_ENV = nil -- or M

-- Local Variables for module-only access
--
local UPDATES	 = 'tcp://*:5610'
--local UPSTREAM   = 'tcp://*:5060'
local SPIES	 = 'inproc://espias'
local SKS	 = {}

local secret = "hjLXIbvtt/N57Ara]e!@gHF=}*n&g$odQVsNG^jb"

-- Local function definitions --
--------------------------------
--


---------------------------------
-- Program execution statement --
---------------------------------
--
-- Initialize servers
local CTX = context()

local ups = assert(CTX:socket'ROUTER')
--[[ -- -- -- -- --
-- * MONITOR *
local spy = assert(CTX:socket'PAIR')
assert( server:monitor( SPIES ) )
assert( spy:connect( SPIES ) )
-- -- -- -- -- --]]
-- ***********
--assert( ups:notify(false) )

assert( ups:curve( secret ) )

assert( ups:bind( UPDATES ) )

print('\nSuccessfully bound to:', UPDATES, '\n')
--[[ -- -- -- -- --
--
local msgs = assert(CTX:socket'PULL')

assert( msgs:bind( UPSTREAM ) )

print('\nSuccessfully bound to:', UPSTREAM, '\n')

---[[
--]]
print( 'Starting servers ...', '\n' )
--sleep(1)


--
while true do

    print'+\n'

    if pollin{ups} then -- msgs, spy

	if ups:events() == 'POLLIN' then
	    local id, msg = receive( ups )
	    msg = msg[1]
	    print( id, msg )
	end

    end
end
---]]


--[[

--]]
