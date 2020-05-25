require "bson"
require "../commands"

module Mongo::Commands::IsMaster
  extend self

  OS_TYPE = {%if flag?(:linux)%} "Linux" {%elsif flag?(:darwin)%} "Darwin" {%elsif flag?(:win32)%} "Windows" {% else %} "Unknown" {% end %}

  def command
    { BSON.new({
      isMaster: 1,
      "$db": "admin",
      client: {
        application: {
            name: "mongo-crystal-driver"
        },
        driver: {
            name: "mongo-crystal-driver",
            version: Mongo::VERSION
        },
        os: {
            type: OS_TYPE,
            # name: "unknown",
            # architecture: "unknown",
            # version: "unknown"
        },
        # platform: "<string>"
      }
    }), nil }
  end

  Common.result(Result) {
    property ismaster : Bool
    property max_bson_object_size : Int32 = 16 * 1024 * 1024
    property max_message_size_bytes : Int32 = 48_000_000
    property max_write_batch_size : Int32 = 100_000
    property local_time : Time
    property logical_session_timeout_minutes : Int32?
    property connection_id : Int32?
    property min_wire_version : Int32
    property max_wire_version : Int32
    property read_only : Bool?
    property compression : Array(String)?
    property sasl_supported_mechs : Array(String)?

    # Sharded instances
    property msg : String?

    # Replica sets
    property hosts : Array(String)?
    property set_name : String?
    property set_version : String?
    property secondary : Bool?
    property passives : Array(String)?
    property arbiters : Array(String)?
    property primary : String?
    property arbiter_only : Bool?
    property passive : Bool?
    property hidden : Bool?
    property tags : BSON?
    property me : String?
    property election_id : BSON::ObjectId?
    property last_write : BSON?
  }

  def result(bson)
    Result.from_bson bson
  end
end
