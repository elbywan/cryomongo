require "log"
require "bson"
require "./cryomongo/messages/**"
require "./cryomongo/client"
require "./cryomongo/gridfs"

module Mongo
  VERSION = "0.1.0"

  Log = ::Log.for(self)
end
