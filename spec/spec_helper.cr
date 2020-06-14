require "spec"
require "log"
require "../src/cryomongo"

Log.setup_from_env

SERVER_VERSION = SemanticVersion.parse(ENV["MONGODB_VERSION"]? || "4.2.0")

def start_mongo
  mongo_path = ENV["MONGODB_PATH"]?
  binary_path_option = mongo_path ? "--binarypath #{mongo_path}" : ""
  puts `mlaunch init --single #{binary_path_option}`
end

def stop_mongo
  puts `mlaunch stop`
end
