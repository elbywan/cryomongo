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
  ShardedMultipleMongos
end

def try(command, *, times = 3, delay = 1.seconds)
  i = 0
  while i < times
    process = Process.new(command, shell: true, input: Process::Redirect::Inherit, output: Process::Redirect::Pipe, error: Process::Redirect::Inherit)
    output = process.output.gets_to_end
    exit_status = process.wait
    puts output
    break if exit_status.normal_exit?
    puts "Exit code in error: #{exit_status.exit_code}"
    puts "Retrying in #{delay}…"
    sleep delay
    i += 1
  end
end

def start_mongo(topology : MongoLaunchTopology = :single)
  topology_argument = case topology
                      when MongoLaunchTopology::Single
                        "--single"
                      when MongoLaunchTopology::Replicaset
                        "--replicaset"
                      when MongoLaunchTopology::Sharded
                        "--replicaset --sharded 3 --port 27017"
                      when MongoLaunchTopology::ShardedMultipleMongos
                        "--replicaset --sharded 3 --mongos 2 --port 27017"
                      end

  mongo_path = ENV["MONGODB_PATH"]?
  binary_path_option = mongo_path ? "--binarypath #{mongo_path}" : ""
  puts `mlaunch init --setParameter enableTestCommands=1 #{topology_argument} #{binary_path_option}`
end

def stop_mongo
  try("mlaunch stop")
  puts `mlaunch kill --signal SIGKILL`
  sleep 1.seconds
end

def with_mongo(topologies = nil, &block : (Proc(Mongo::Client), MongoLaunchTopology, String) -> Nil)
  MongoLaunchTopology.each { |topology|
    next if topologies.try { |t| !topology.in?(t) }
    context "in #{topology} mode", tags: "#{topology.to_s.underscore}" do
      client = uninitialized Mongo::Client
      get_client : -> Mongo::Client = ->{ client }
      uri = topology.sharded_multiple_mongos? ? "mongodb://localhost:27017,localhost:27018/" : "mongodb://localhost:27017/"

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

      block.call(get_client, topology, uri)
    end
  }
end

require "./runner"
