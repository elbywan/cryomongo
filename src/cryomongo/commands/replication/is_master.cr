require "bson"
require "../commands"

module Mongo::Commands::IsMaster
  extend self

  OS_TYPE = {% if host_flag?(:linux) %} "Linux" {% elsif host_flag?(:darwin) %} "Darwin" {% elsif host_flag?(:win32) %} "Windows" {% else %} "Unknown" {% end %}

  def command(appname : String? = nil)
    {BSON.new({
      isMaster: 1,
      "$db":    "admin",
      client:   {
        application: {
          name: appname || "cryomongo",
        },
        driver: {
          name:    "cryomongo",
          version: Mongo::VERSION,
        },
        os: {
          type: OS_TYPE,
          # name: "unknown",
          # architecture: "unknown",
          # version: "unknown"
        },
        # platform: "<string>"
      },
    }), nil}
  end

  Common.result(Result) {
    property ismaster : Bool = false
    property max_bson_object_size : Int32 = 16 * 1024 * 1024
    property max_message_size_bytes : Int32 = 48_000_000
    property max_write_batch_size : Int32 = 100_000
    property local_time : Time?
    property logical_session_timeout_minutes : Int32?
    property connection_id : Int32?
    property min_wire_version : Int32 = 0
    property max_wire_version : Int32 = 0
    property read_only : Bool?
    property compression : Array(String)?
    property sasl_supported_mechs : Array(String)?

    # Sharded instances
    property msg : String?

    # Replica sets
    property hosts : Array(String)?
    property set_name : String?
    property set_version : Int32?
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
    property isreplicaset : Bool?

    # Custom
    property null_logical_session_timeout_minutes : Bool = false
  }

  def result(bson)
    result = Result.from_bson(bson)
    result.null_logical_session_timeout_minutes = bson.has_key?("logicalSessionTimeoutMinutes") && result.logical_session_timeout_minutes.nil?
    result
  end
end
