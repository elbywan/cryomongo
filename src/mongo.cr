require "log"
require "bson"
require "./messages/**"
require "./client"

module Mongo
  VERSION = "0.1.0"

  Log = ::Log.for(self)
end
