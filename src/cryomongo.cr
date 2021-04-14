require "log"
require "bson"
require "./cryomongo/messages/**"
require "./cryomongo/client"
require "./cryomongo/gridfs"

# The main Cryomongo module.
module Mongo
  VERSION = "0.3.6"

  Log = ::Log.for(self)
end
