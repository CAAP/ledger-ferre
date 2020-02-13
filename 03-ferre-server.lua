#! /usr/bin/env lua53

-- Import Section
--
local fd	  = require'carlos.fold'

local receive	  = require'carlos.ferre'.receive
local send	  = require'carlos.ferre'.send
local getFruit	  = require'carlos.ferre'.getFruit
local pollin	  = require'lzmq'.pollin
local context	  = require'lzmq'.context

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

--------------------------------
-- Local function definitions --
--------------------------------
--


---------------------------------
-- Program execution statement --
---------------------------------
--
-- Initialize servers
local CTX = context()

local server = assert(CTX:socket'ROUTER')
--[[ -- -- -- -- --
-- * MONITOR *
local spy = assert(CTX:socket'PAIR')
assert( server:monitor( SPIES ) )
assert( spy:connect( SPIES ) )
-- -- -- -- -- --]]
-- ***********
assert( server:notify(false) )

assert( server:bind( UPDATES ) )

print('\nSuccessfully bound to:', UPDATES, '\n')
--[[ -- -- -- -- --
--
local msgs = assert(CTX:socket'PULL')

assert( msgs:bind( UPSTREAM ) )

print('\nSuccessfully bound to:', UPSTREAM, '\n')

---[[
--]]
print( 'Starting servers ...', '\n' )
sleep(1)


--
while true do

    print'+\n'

    if pollin{msgs} then -- , spy

	if msgs:events() == 'POLLIN' then
	    print( switch(msgs, server), '\n' )
	end

	if spy:events() == 'POLLIN' then
	    local ev, mm = receive(spy)
	    print( ev, '\n' )
	    if mm[1]:match'tcp' then
		local sk = toint(ev:match'%d+$')
		if ev:match'DISCONNECTED' then
--		    print( ev, '\n' )
		    print( 'Bye bye', sayonara(sk), '\n')
		elseif ev:match'ACCEPTED' then
--		    print( ev, '\n' )
		    print( handshake(server, sk), '\n' )
		end
	    end
	end

    end
end
---]]


--[[

--]]
