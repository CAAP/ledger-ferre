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
local DOWNSTREAM = 'ipc://downstream.ipc' --  

local UPDATES	 = 'tcp://*:5610'
local SKS	 = {["FA-BJ-01"]=true}

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
-- -- -- -- -- --]]
-- ***********

assert( ups:curve( secret ) )

assert( ups:bind( UPDATES ) )

print('\nSuccessfully bound to:', UPDATES, '\n')
---[[ -- -- -- -- --
--
local tasks = assert(CTX:socket'PUB')

assert(tasks:bind( DOWNSTREAM ))

print('Successfully bound to:', DOWNSTREAM, '\n')
---[[
--]]

--
while true do

    print'+\n'

    if pollin{ups, tasks} then -- msgs, spy

	if ups:events() == 'POLLIN' then
	    local id, msg = receive( ups )

	    if SKS[id] then
		msg = msg[1] -- XXX assumes one-message only
		tasks:send_msg( msg )
		print( id, msg )
	    end

	end

    end
end
---]]


--[[

--]]
