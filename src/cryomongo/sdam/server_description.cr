require "../commands/replication/is_master"

class Mongo::SDAM::ServerDescription
  enum ServerType
    Standalone
    Mongos
    PossiblePrimary
    RSPrimary
    RSSecondary
    RSArbiter
    RSOther
    RSGhost
    Unknown
  end

  # The hostname or IP, and the port number that the client connects to.
  getter address : String
  # Information about the last error related to this server
  property error : String? = nil
  # The duration of the ismaster call.
  getter round_trip_time : Time::Span = 0.seconds
  # A 64-bit BSON datetime or null. The "lastWriteDate" from the server's most recent ismaster response.
  getter last_write_date : Time? = nil
  # An opaque value representing the position in the oplog of the most recently seen write.
  # (Only mongos and shard servers record this field when monitoring config servers as replica sets,
  # at least until drivers allow applications to use readConcern "afterOptime".)
  getter op_time : BSON? = nil
  # A ServerType enum value.
  property type : ServerType = :unknown
  # The wire protocol version range supported by the server.
  # Use min and maxWireVersion only to determine compatibility.
  getter min_wire_version : Int32 = 0
  getter max_wire_version : Int32 = 0
  # The hostname or IP, and the port number, that this server was configured with in the replica set.
  getter me : String? = nil
  # Sets of addresses. This server's opinion of the replica set's members, if any.
  # These hostnames are normalized to lower-case.
  # The client monitors all three types of servers in a replica set.
  getter hosts : Array(String)? = [] of String
  getter passives : Array(String)? = [] of String
  getter arbiters : Array(String)? = [] of String
  # Map from string to string.
  getter tags : BSON? # Hash(String, String) = {} of String => String
  getter set_name : String? = nil
  getter set_version : Int32? = nil
  # An ObjectId, if this is a MongoDB 2.6+ replica set member that believes it is primary.
  # See using setVersion and electionId to detect stale primaries.
  getter election_id : BSON::ObjectId? = nil
  # This server's opinion of who the primary is.
  getter primary : String? = nil
  # When this server was last checked.
  property last_update_time : Time = Time::UNIX_EPOCH
  getter logical_session_timeout_minutes : Int32? = nil
  getter null_logical_session_timeout_minutes : Bool = false
  # The "topologyVersion" from the server's most recent ismaster response or State Change Error.
  getter topology_version : BSON? = nil

  def initialize(@address : String)
  end

  private macro from_is_master(fields, is_master)
    {% for field in fields %}
      @{{field.id}} = {{is_master.id}}.{{field.id}}
    {% end %}
  end

  def initialize(address : String, is_master_result : Commands::IsMaster::Result, @round_trip_time : Time::Span)
    @address = address.downcase
    from_is_master(%w(
      min_wire_version
      max_wire_version
      logical_session_timeout_minutes
      set_name
      set_version
      primary
      hosts
      passives
      arbiters
      me
      election_id
      tags
      null_logical_session_timeout_minutes
    ), is_master_result)

    @last_update_time = Time.utc
    @last_write_date = is_master_result.last_write.try &.["lastWriteDate"]?.try &.as(Time)
    @op_time = is_master_result.last_write.try &.["opTime"]?.try &.as(BSON)

    if is_master_result.msg === "isdbgrid"
      @type = :mongos
    elsif is_master_result.isreplicaset
      @type = :rs_ghost
    elsif is_master_result.set_name.nil?
      @type = :standalone
    elsif is_master_result.ismaster
      @type = :rs_primary
    elsif is_master_result.hidden
      @type = :rs_other
    elsif is_master_result.secondary
      @type = :rs_secondary
    elsif is_master_result.arbiter_only
      @type = :rs_arbiter
    else
      @type = :rs_other
    end
  end

  def update(other : ServerDescription)
    {% begin %}
      {% for ivar in @type.instance_vars %}
        @{{ivar.id}} = other.{{ivar.id}}
      {% end %}
    {% end %}
  end

  def ==(other : ServerDescription)
    other.address == @address &&
      other.error == @error &&
      other.type == @type &&
      other.min_wire_version == @min_wire_version &&
      other.max_wire_version == @max_wire_version &&
      other.me == @me &&
      other.hosts == @hosts &&
      other.passives == @passives &&
      other.arbiters == @arbiters &&
      other.tags == @tags &&
      other.set_name == @set_name &&
      other.set_version == @set_version &&
      other.election_id == @election_id &&
      other.primary == @primary &&
      other.logical_session_timeout_minutes == @logical_session_timeout_minutes &&
      other.topology_version == @topology_version
  end

  def !=(other : ServerDescription)
    !self.==(other)
  end

  def data_bearing?
    @type.mongos? || @type.rs_primary? || @type.rs_secondary? || @type.standalone?
  end

  def primary_or_possible?
    self.type.rs_primary? || self.type.possible_primary?
  end

  def unknown_or_ghost?
    self.type.unknown? || self.type.rs_ghost?
  end

  def supports_retryable_writes?
    self.max_wire_version >= 6 &&
    self.logical_session_timeout_minutes &&
    !self.type.standalone?
  end

  def supports_retryable_reads?
    self.max_wire_version >= 6
  end
end
