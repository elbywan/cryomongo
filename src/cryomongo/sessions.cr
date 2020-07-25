require "uuid"

module Mongo::Session
  # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst

  @[BSON::Options(camelize: "lower")]
  struct ClusterTime
    include Comparable(ClusterTime)
    include BSON::Serializable

    getter cluster_time : BSON::Timestamp
    getter signature : Signature

    @[BSON::Options(camelize: "lower")]
    struct Signature
      include BSON::Serializable

      getter hash : Bytes
      getter key_id : Int64
    end

    def <=>(other : ClusterTime)
      self.cluster_time <=> other.cluster_time
    end
  end

  struct SessionId
    include BSON::Serializable
    include Tools::Initializer
    getter id : UUID
  end

  record Options, causal_consistency : Bool? = nil

  class ClientSession
    @client : Mongo::Client
    @server_session : ServerSession
    @released = false
    @lock = Mutex.new

    getter cluster_time : ClusterTime? = nil
    getter operation_time : BSON::Timestamp? = nil
    getter options : Options
    getter? implicit : Bool = true

    delegate :dirty, :txn_number, to: @server_session
    protected delegate :dirty=, to: @server_session

    def initialize(@client : Mongo::Client, @implicit = true, **options : **U) forall U
      {% begin %}
      {{ @type }} # see: https://github.com/crystal-lang/crystal/issues/2731
      @options = Options.new(
        {% for k in U %}
          {{ k.id }}: options[{{ k.symbolize }}],
        {% end %}
      )
      @server_session = @client.session_pool.acquire(logical_timeout).not_nil!
      {% end %}
    end

    def session_id
      @server_session.session_id
    end

    def advance_cluster_time(cluster_time : ClusterTime)
      self_cluster_time = @cluster_time
      if !self_cluster_time || self_cluster_time < cluster_time
        @cluster_time = cluster_time
      end
    end

    def advance_operation_time(operation_time : BSON::Timestamp)
      self_operation_time = @operation_time
      if !self_operation_time || self_operation_time < operation_time
        @operation_time = operation_time
      end
    end

    def end
      return if @released
      @lock.synchronize {
        @released = true
        @client.session_pool.release(@client, @server_session, logical_timeout)
      }
    end

    def logical_timeout
      @client.topology.logical_session_timeout_minutes.try(&.minutes) || 30.minutes
    end

    def increment_txn_number
      @lock.synchronize {
        @server_session.txn_number += 1
      }
    end
  end

  class ServerSession
    getter session_id : SessionId
    getter last_use : Time? = nil
    property dirty : Bool = false
    property txn_number : Int64 = 0

    def initialize
      id = UUID.random
      @session_id = SessionId.new(id: id)
    end

    def use
      @last_use = Time.utc
    end

    def stale?(logical_timeout : Time::Span)
      @last_use.try { |use| use + logical_timeout <= Time.utc + 1.minute }
    end
  end

  # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#server-session-pool
  class Pool
    @mutex = Mutex.new(:reentrant)
    @closed : Bool = false
    @pool : Deque(ServerSession) = Deque(ServerSession).new

    def acquire(logical_timeout : Time::Span)
      raise Mongo::Error.new("Client is closed.") if @closed
      # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#algorithm-to-acquire-a-serversession-instance-from-the-server-session-pool
      @mutex.synchronize {
        loop do
          if session = @pool.shift?
            unless session.stale?(logical_timeout)
              return session
            end
          else
            return ServerSession.new
          end
        end
      }
    end

    def release(client : Mongo::Client, session : ServerSession, logical_timeout : Time::Span)
      if @closed
        begin
          client.command(Commands::EndSessions, ids: [session.session_id])
        rescue
          # ignore - client could have been closed too
        end
        return
      end
      # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#algorithm-to-return-a-serversession-instance-to-the-server-session-pool
      @mutex.synchronize {
        loop do
          if (last = @pool.last?) && last.stale?(logical_timeout)
            @pool.pop
          else
            break
          end
        end

        unless session.dirty || session.stale?(logical_timeout)
          @pool.unshift session
        end
      }
    end

    def close(client : Mongo::Client)
      return if @closed
      # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#endsessions
      # close the pool and end the sessions - by batches of 10_000
      @mutex.synchronize {
        @pool.each.map(&.session_id).each_slice(10_000) do |ids|
          client.command(Commands::EndSessions, ids: ids)
        end
        @closed = true
      }
    end
  end
end
