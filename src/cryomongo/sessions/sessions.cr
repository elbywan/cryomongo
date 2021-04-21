require "uuid"
require "bson"
require "../tools"
require "../client"

# Contains all the logic related to server or client sessions.
#
# Version 3.6 of the server introduces the concept of logical sessions for clients.
# A session is an abstract concept that represents a set of sequential operations executed by an application that are related in some way.
#
# See: [https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst](https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst)
module Mongo::Session
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

  record Options,
    causal_consistency : Bool? = nil,
    # The default TransactionOptions to use for transactions started on this session.
    default_transaction_options : TransactionOptions? = nil

  # A client session used to logically bind operations together.
  class ClientSession
    @client : Mongo::Client
    @server_session : ServerSession
    @released = false
    @lock = Mutex.new

    # This property returns the most recent cluster time seen by this session.
    # If no operations have been executed using this session this value will be null unless advanceClusterTime has been called.
    # This value will also be null when a cluster does not report cluster times.
    getter cluster_time : ClusterTime? = nil
    # This property returns the operation time of the most recent operation performed using this session.
    # If no operations have been performed using this session the value will be null unless advanceOperationTime has been called.
    # This value will also be null when the cluster does not report operation times.
    getter operation_time : BSON::Timestamp? = nil
    # The session options used when creating the session.
    getter options : Options

    protected getter? implicit : Bool = true
    protected delegate :dirty, :dirty=, :txn_number, :session_id, to: @server_session

    protected def initialize(@client : Mongo::Client, @implicit = true, **options : **U) forall U
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

    # This method advances the *cluster_time*.
    #
    # NOTE: this method is a no-op if the provider cluster time is less than the current cluster time.
    def advance_cluster_time(cluster_time : ClusterTime)
      self_cluster_time = @cluster_time
      if !self_cluster_time || self_cluster_time < cluster_time
        @cluster_time = cluster_time
      end
    end

    # This method advances the *operation_time*.
    #
    # NOTE: this method is a no-op if the provider operation time is less than the current operation time.
    def advance_operation_time(operation_time : BSON::Timestamp)
      self_operation_time = @operation_time
      if !self_operation_time || self_operation_time < operation_time
        @operation_time = operation_time
      end
    end

    # Terminate the session and return it to the pool.
    def end
      return if @released
      @lock.synchronize {
        @released = true
        @client.session_pool.release(@client, @server_session, logical_timeout)
      }
    end

    protected def logical_timeout
      @client.topology.logical_session_timeout_minutes.try(&.minutes) || 30.minutes
    end

    protected def increment_txn_number
      # @lock.synchronize {
      @server_session.txn_number += 1
      # }
    end
  end

  private class ServerSession
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

  # :nodoc:
  # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#server-session-pool
  class Pool
    @lock = Mutex.new(:reentrant)
    @closed : Bool = false
    @pool : Deque(ServerSession) = Deque(ServerSession).new

    def acquire(logical_timeout : Time::Span)
      raise Mongo::Error.new("Client is closed.") if @closed
      # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#algorithm-to-acquire-a-serversession-instance-from-the-server-session-pool
      @lock.synchronize {
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
      @lock.synchronize {
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
      @lock.synchronize do
        @pool.each.map(&.session_id).each_slice(10_000) do |ids|
          client.command(Commands::EndSessions, ids: ids)
        end
      ensure
        @closed = true
      end
    end
  end
end

require "./transactions"
