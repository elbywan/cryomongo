require "semantic_version"
require "spec"
require "log"
require "../src/cryomongo"

SERVER_VERSION = SemanticVersion.parse(ENV["MONGODB_VERSION"]? || "4.2.0")

class Mongo::Client
  # speeds up tests in replica set mode - see: https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests#speeding-up-tests
  @min_heartbeat_frequency = 50.milliseconds
end

def semantic(str)
  split = str.split(".")
  full_version = ""
  3.times do |i|
    if i == 0
      full_version += (split[i]? || "0")
    else
      full_version += ("." + (split[i]? || "0"))
    end
  end
  SemanticVersion.parse full_version
end

enum MongoLaunchTopology
  Single
  Replicaset
  Sharded
end

def start_mongo(topology : MongoLaunchTopology = :single)
  topology_argument = case topology
  when MongoLaunchTopology::Single
    "--single"
  when MongoLaunchTopology::Replicaset
    "--replicaset"
  when MongoLaunchTopology::Sharded
    "--replicaset --sharded 2 --port 27017"
  end


  mongo_path = ENV["MONGODB_PATH"]?
  binary_path_option = mongo_path ? "--binarypath #{mongo_path}" : ""
  puts `mlaunch init --setParameter enableTestCommands=1 #{topology_argument} #{binary_path_option}`
end

def stop_mongo
  puts `mlaunch stop`
end

def with_mongo(&block : ((-> Mongo::Client), MongoLaunchTopology) -> Nil)
  MongoLaunchTopology.each { |topology|
    context "in #{topology} mode", tags: "#{topology.to_s.underscore}" do
      client = uninitialized Mongo::Client
      get_client : -> Mongo::Client = -> { client }

      before_all {
        `rm -Rf ./data`
        start_mongo(topology)
        client = Mongo::Client.new
      }

      after_all {
        client.close
        stop_mongo
        `rm -Rf ./data`
      }

      block.call(get_client, topology)
    end
  }
end

require "./runner"
