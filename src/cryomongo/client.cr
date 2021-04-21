require "socket"
require "db"
require "./database"
require "./messages/**"
require "./commands/**"
require "./error"
require "./concerns"
require "./read_preference"
require "./sdam/**"
require "./uri"
require "./monitoring"

# The client which provides access to a MongoDB server, replica set, or sharded cluster.
#
# It maintains management of underlying sockets and routing to individual nodes.
class Mongo::Client
  include WithReadConcern
  include WithWriteConcern
  include WithReadPreference

  alias NetworkError = IO::Error | Socket::Error

  # The mininum wire protocol version supported by this driver.
  MIN_WIRE_VERSION = 6
  # The maximum wire protocol version supported by this driver.
  MAX_WIRE_VERSION = 8

  # :nodoc:
  getter! topology : SDAM::TopologyDescription
  # The set of driver options.
  getter options : Options
  # The current highest seen cluster time for the deployment
  getter cluster_time : Session::ClusterTime?
  # :nodoc:
  getter session_pool : Session::Pool = Session::Pool.new
  # :nodoc:
  protected getter min_heartbeat_frequency : Time::Span = 500.milliseconds

  @@lock = Mutex.new(:reentrant)
  @pools : Hash(String, DB::Pool(Mongo::Connection)) = Hash(String, DB::Pool(Mongo::Connection)).new
  @monitors : Array(SDAM::Monitor) = Array(SDAM::Monitor).new
  @socket_check_interval : Time::Span = 5.seconds
  @last_scan : Time = Time::UNIX_EPOCH
  @topology_update = Channel(Nil).new
  @commands_observable = Monitoring::Observable(Monitoring::Commands::Event).new

  # The default auth database is optionally provided as a part of the connection string uri.
  #
  # see: https://docs.mongodb.com/manual/reference/connection-string/
  getter default_auth_db : String

  # Create a mongodb client instance from a mongodb URL.
  #
  # ```
  # require "cryomongo"
  #
  # client = Mongo::Client.new "mongodb://127.0.0.1/?appname=client-example"
  # ```
  def initialize(connection_string : String = "mongodb://localhost:27017", options : Mongo::Options = Mongo::Options.new)
    initialize(connection_string: connection_string, options: options, start_monitoring: true)
  end

  # :nodoc:
  def initialize(connection_string : String = "mongodb://localhost:27017", *, options : Mongo::Options = Mongo::Options.new, start_monitoring = true)
    seeds, @options, @credentials, @default_auth_db = Mongo::URI.parse(connection_string, options)

    if (w = @options.w) || (w_timeout = @options.w_timeout) || (journal = @options.journal)
      @write_concern = WriteConcern.new(w: w, w_timeout: w_timeout.try &.milliseconds.to_i64, j: journal)
    end

    if read_concern_level = @options.read_concern_level
      @read_concern = ReadConcern.new(level: read_concern_level)
    end

    if read_pref = @options.read_preference
      @read_preference = ReadPreference.new(
        mode: read_pref,
        max_staleness_seconds: @options.max_staleness_seconds,
        tags: @options.read_preference_tags.map { |tags|
          bson = BSON.new
          tags.split(',').each { |tag|
            key, value = tag.split(':')
            bson[key] = value
          }
          bson
        }
      )
    end

    @topology = SDAM::TopologyDescription.new(self, seeds.map(&.address), @options)
    topology.servers.each { |server|
      add_monitor(server, start_monitoring: start_monitoring)
    }
  end

  # Frees all the resources associated with a client.
  def close
    @pools.each do |_, pool|
      pool.close
    rescue e
      Log.warn { "Error while trying to close connection pool. #{e}" }
    end
    begin
      @session_pool.close(self)
    rescue e
      Log.warn { "Error while trying to close session pool. #{e}" }
    end
    @monitors.each do |monitor|
      monitor.close
    rescue e
      Log.warn { "Error while trying to close monitor fiber. #{e}" }
    end
  end

  ##################
  # Public Methods #
  ##################

  # Get a newly allocated `Mongo::Database` for the database named *name*.
  def database(name : String) : Database
    Database.new(self, name)
  end

  # Get a newly allocated `Mongo::Database`using the default auth database string
  # optionally provided as a part of the connection string uri.
  #
  # see: https://docs.mongodb.com/manual/reference/connection-string/
  def default_database : Database?
    self.database(name: @default_auth_db) unless @default_auth_db.empty?
  end

  # :ditto:
  def [](name : String) : Database
    database(name)
  end

  # Execute a command on the server.
  #
  # ```
  # # First argument is the `Mongo::Commands`.
  # client.command(Mongo::Commands::DropDatabase, database: "database_name")
  # ```
  def command(
    command,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    server_description : SDAM::ServerDescription? = nil,
    session : Session::ClientSession? = nil,
    operation_id : Int64? = nil,
    **args,
    &block
  )
    # Create an implicit session
    session ||= Session::ClientSession.new(self)

    result = begin
      if session && session.is_transaction? && !command.is_a?(Commands::CommitTransaction) && !command.is_a?(Commands::AbortTransaction)
        session.insert_transaction {
          internal_command(
            command,
            **args,
            write_concern: write_concern,
            read_concern: read_concern,
            read_preference: read_preference,
            server_description: server_description,
            session: session,
            operation_id: operation_id,
          )
        }
      else
        internal_command(
          command,
          **args,
          write_concern: write_concern,
          read_concern: read_concern,
          read_preference: read_preference,
          server_description: server_description,
          session: session,
          operation_id: operation_id,
        )
      end
    end
    result.try { |r|
      yield r, session # , server_description
    }
  rescue e
    if command.is_a? Commands::AbortTransaction
      # Ignore abort transaction errors
      # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#drivers-ignore-all-aborttransaction-errors
      return nil
    end

    raise e
  end

  # :ditto:
  def command(
    command cmd,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    server_description : SDAM::ServerDescription? = nil,
    session : Session::ClientSession? = nil,
    operation_id : Int64? = nil,
    **args
  )
    self.command(cmd, write_concern, read_concern, read_preference, server_description, session, operation_id, **args) { |result|
      result
    }
  end

  private def internal_command(
    command,
    write_concern : WriteConcern? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    server_description : SDAM::ServerDescription? = nil,
    session : Session::ClientSession? = nil,
    operation_id : Int64? = nil,
    **args
  )
    # Mix collection/database/client/options read and write concerns considering the precedence rules.
    # args = args.merge({
    #   options: args["options"]? || NamedTuple.new,
    # })
    args = WithWriteConcern.mix_write_concern(command, args, write_concern || @write_concern, session: session)
    args = WithReadConcern.mix_read_concern(command, args, read_concern || @read_concern, session: session)

    # Determines the read preference to apply to the command
    if WithReadPreference.must_use_primary_command?(command, args)
      read_preference = ReadPreference.new(mode: "primary")
    else
      if session.is_transaction?
        read_preference = session.current_transaction_options.read_preference || read_preference || @read_preference || ReadPreference.new(mode: "primary")
      else
        read_preference = read_preference || @read_preference || ReadPreference.new(mode: "primary")
      end
    end

    # See: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#readpreference
    if session.is_transaction? && read_preference.mode != "primary"
      raise Error::Transaction.new("read preference in a transaction must be primary.")
    end

    # Determine whether the request is acknowledged and prohibit some operations.
    acknowledged = acknowledged?(args, session)

    # Session could be pinned to a specific mongos - if so use the same server description
    server_description ||= session.server_description

    retryable_command = acknowledged && command.is_a?(Commands::Retryable) && command.retryable?(**args, session: session)

    if (retryable_command && @options.retry_writes || command.is_a?(Commands::AlwaysRetryable)) && command.is_a?(Commands::WriteCommand) && command.write_command?
      execute_retryable_write(
        command,
        session,
        read_preference,
        server_description,
        operation_id,
        **args
      )
    elsif retryable_command && @options.retry_reads && command.is_a?(Commands::ReadCommand) && command.read_command?
      execute_retryable_read(
        command,
        session,
        read_preference,
        server_description,
        operation_id,
        **args
      )
    else
      # Select a suitable server and retrieve the underlying connection.
      server_description ||= server_selection(command, args, read_preference)
      connection = get_connection(server_description)
      session.pin(server_description)

      execute_command(
        command,
        session,
        read_preference,
        server_description,
        connection,
        operation_id,
        **args
      )
    end
  end

  # Provides a list of all existing databases along with basic statistics about them.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/listDatabases).
  def list_databases(
    *,
    filter = nil,
    name_only : Bool? = nil,
    authorized_databases : Bool? = nil,
    session : Session::ClientSession? = nil
  ) : Commands::ListDatabases::Result
    self.command(Commands::ListDatabases, session: session, options: {
      filter:               filter,
      name_only:            name_only,
      authorized_databases: authorized_databases,
    }).not_nil!
  end

  # Returns a document that provides an overview of the database’s state.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/serverStatus/).
  def status(*, repl : Int32? = nil, metrics : Int32? = nil, locks : Int32? = nil, mirrored_reads : Int32? = nil, latch_analysis : Int32? = nil, session : Session::ClientSession? = nil) : BSON?
    self.command(Commands::ServerStatus, session: session, options: {
      repl:           repl,
      metrics:        metrics,
      locks:          locks,
      mirrored_reads: mirrored_reads,
      latch_analysis: latch_analysis,
    })
  end

  # An administrative command that returns usage statistics for each collection.
  #
  # NOTE: [for more details, please check the official MongoDB documentation](https://docs.mongodb.com/manual/reference/command/top).
  def top : BSON?
    self.command(Commands::Top)
  end

  # Allows a client to observe all changes in a cluster.
  #
  # Returns a change stream on all collections in all databases in a cluster.
  #
  # NOTE: Excludes system collections.
  def watch(
    pipeline : Array = [] of BSON,
    *,
    full_document : String? = nil,
    resume_after : BSON? = nil,
    max_await_time_ms : Int64? = nil,
    batch_size : Int32? = nil,
    collation : Collation? = nil,
    start_at_operation_time : Time? = nil,
    start_after : BSON? = nil,
    read_concern : ReadConcern? = nil,
    read_preference : ReadPreference? = nil,
    session : Session::ClientSession? = nil
  ) : Mongo::ChangeStream::Cursor
    ChangeStream::Cursor.new(
      client: self,
      database: "admin",
      collection: 1,
      pipeline: pipeline.map { |elt| BSON.new(elt) },
      full_document: full_document,
      resume_after: resume_after,
      start_after: start_after,
      start_at_operation_time: start_at_operation_time,
      read_concern: read_concern,
      read_preference: read_preference,
      max_time_ms: max_await_time_ms,
      batch_size: batch_size,
      collation: collation,
      session: session
    )
  end

  # Starts a new logical session for a sequence of operations.
  #
  # ```
  # client = Mongo::Client.new
  #
  # # First, create a ClientSession which is by default causally consistent.
  # session = client.start_session
  # collection = client["db"]["coll"]
  #
  # # On a side note, it is important to ensure that both read and writes are performed with "majority" concern.
  # collection.read_concern = Mongo::ReadConcern.new(level: "majority")
  # collection.write_concern = Mongo::WriteConcern.new(w: "majority")
  #
  # # Then pass session as the *session* named argument…
  # collection.insert_one({a: 1}, session: session)
  # collection.find_one({a: 1}, session: session)
  #
  # # …and always end the session after using it.
  # session.end
  # ```
  def start_session(*,
                    causal_consistency : Bool = true,
                    default_transaction_options : Session::TransactionOptions? = nil) : Session::ClientSession
    Session::ClientSession.new(
      client: self,
      implicit: false,
      causal_consistency: causal_consistency,
      default_transaction_options: default_transaction_options
    )
  end

  # Subscribe to monitoring command events.
  #
  # ```
  # client = Mongo::Client.new
  #
  # client.subscribe_commands { |event|
  #   case event
  #   when Mongo::Monitoring::Commands::CommandStartedEvent
  #     Log.info { "COMMAND.#{event.command_name} #{event.address} STARTED: #{event.command.to_json}" }
  #   when Mongo::Monitoring::Commands::CommandSucceededEvent
  #     Log.info { "COMMAND.#{event.command_name} #{event.address} COMPLETED: #{event.reply.to_json} (#{event.duration}s)" }
  #   when Mongo::Monitoring::Commands::CommandFailedEvent
  #     Log.info { "COMMAND.#{event.command_name} #{event.address} FAILED: #{event.failure.inspect} (#{event.duration}s)" }
  #   end
  # }
  # ```
  def subscribe_commands(&callback : Monitoring::Commands::Event -> Nil) : Monitoring::Commands::Event -> Nil
    @commands_observable.subscribe(&callback)
  end

  # Ends the subscription for command events.
  #
  # ```
  # client = Mongo::Client.new
  #
  # subscription = client.subscribe_commands { |event|
  #   puts event
  # }
  #
  # client.unsubscribe_commands(subscription)
  # ```
  def unsubscribe_commands(callback : Monitoring::Commands::Event -> Nil) : Nil
    @commands_observable.unsubscribe(callback)
  end

  ############
  # Internal #
  ############

  private def execute_command(
    command,
    session : Session::ClientSession,
    read_preference : ReadPreference,
    server_description : SDAM::ServerDescription,
    connection : Mongo::Connection,
    operation_id : Int64? = nil,
    **args
  )
    execute_command(command, session, read_preference, server_description, connection, operation_id, **args) { }
  end

  private def execute_command(
    command,
    session : Session::ClientSession,
    read_preference : ReadPreference,
    server_description : SDAM::ServerDescription,
    connection : Mongo::Connection,
    operation_id : Int64? = nil,
    **args
  )
    # Reject for this special case.
    if command == Mongo::Commands::FindAndModify && args["options"]?.try(&.["hint"]?) && server_description.max_wire_version < 8
      raise Mongo::Error.new "The hint option is not supported by MongoDB servers < 4.2"
    end

    # Mix the collection/database/client/options read preferences.
    args = WithReadPreference.mix_read_preference(command, args, read_preference, topology, server_description)

    # Determine whether the request is acknowledged.
    unacknowledged = !acknowledged?(args, session, validate: false)

    # Extract the actual BSON depending on the target command.
    body, sequences = command.command(**args)
    flag_bits = unacknowledged ? Messages::OpMsg::Flags::MoreToCome : Messages::OpMsg::Flags::None

    # Apply session rules.
    if topology.supports_sessions?
      if unacknowledged
        # Sessions are not compatible with unacknowledged writes
        raise Mongo::Error.new("Unacknowledged writes are incompatible with sessions.") unless session.implicit?
      end

      body["lsid"] = session.session_id

      if topology.supports_cluster_time?
        cluster_time = gossip_cluster_time(session)
        body["$clusterTime"] = cluster_time if cluster_time
      end

      if session.is_transaction? && server_description.supports_retryable_writes?
        if session.transitions_from.try(&.starting?)
          body["startTransaction"] = true
        end
        body["txnNumber"] = session.txn_number
        body["autocommit"] = false
      end
    end

    body = (yield body) || body

    # Create the OP_MSG message to send.
    op_msg = Messages::OpMsg.new(body, flag_bits: flag_bits)
    sequences.try &.each { |key, documents|
      op_msg.sequence(key.to_s, contents: documents)
    }

    # Command monitoring related variables.
    duration_start = Time.monotonic
    request_id = uninitialized Int64
    command_name = command.name
    address = connection.server_description.address

    # Send the command.
    connection.send(op_msg, command) { |message|
      # Monitor by sending a CommandStartedEvent
      if @commands_observable.has_subscribers?
        request_id = message.header.request_id.to_i64

        @commands_observable.broadcast(Monitoring::Commands::CommandStartedEvent.new(
          command_name: command_name,
          request_id: request_id,
          operation_id: operation_id,
          address: address,
          command: op_msg.safe_payload(command),
          database_name: op_msg.body["$db"].as(String)
        ))
      end
    }

    # If the write is unacknowledged - early return.
    if unacknowledged
      @commands_observable.broadcast(Monitoring::Commands::CommandSucceededEvent.new(
        command_name: command_name,
        request_id: request_id,
        operation_id: operation_id,
        address: address,
        duration: Time.monotonic - duration_start,
        reply: BSON.new({ok: 1})
      ))

      return nil
    end

    # Receive the server sent OP_MSG.
    op_msg = connection.receive do |message|
      op_msg = message.contents.as(Messages::OpMsg)
      duration = Time.monotonic - duration_start

      # Monitor.
      if @commands_observable.has_subscribers?
        if error = op_msg.error?
          @commands_observable.broadcast(Monitoring::Commands::CommandFailedEvent.new(
            command_name: command_name,
            request_id: message.header.request_id.to_i64,
            operation_id: operation_id,
            address: address,
            duration: duration,
            reply: op_msg.safe_payload(command),
            failure: error
          ))
        else
          @commands_observable.broadcast(Monitoring::Commands::CommandSucceededEvent.new(
            command_name: command_name,
            request_id: message.header.request_id.to_i64,
            operation_id: operation_id,
            address: address,
            duration: duration,
            reply: op_msg.safe_payload(command)
          ))
        end
      end
    end

    # Parse as a base result.
    base_result = Commands::Common::BaseResult.from_bson(op_msg.body)

    # Update the stored cluster time.
    if cluster_time = base_result.cluster_time
      @cluster_time = cluster_time if !@cluster_time || @cluster_time.try &.< cluster_time
      session.advance_cluster_time(cluster_time) if session
    end

    if operation_time = base_result.operation_time
      session.advance_operation_time(operation_time) if session
    end

    # Update the session recovery token if needed.
    # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#recoverytoken-field
    if session.is_transaction? && (token = base_result.recovery_token)
      session.recovery_token = token
    end

    # Raise if the server replied with an error.
    if error = op_msg.error?
      raise error
    end

    # Parse and return the body as a custom Result type.
    result = command.result(op_msg.body)
    result
  rescue error
    if error.is_a?(NetworkError)
      Mongo::Log.error(exception: error) { "Network error" } unless server_description
      # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#network-or-command-error-during-server-check
      server_description.try { |desc|
        Mongo::Log.error(exception: error) { "I/O error with server address: #{desc.address}" }
        description = SDAM::ServerDescription.new(desc.address)
        description.error = error.message
        description.last_update_time = desc.last_update_time
        topology.update(desc, description)
        close_connection_pool(desc)
      }
      session.try &.dirty = true
      error = Error::Network.new(error)
    end

    if error.is_a?(Mongo::Error::Command)
      Mongo::Log.error { "Command error: #{error}" }
      # see: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering
      if error.state_change?
        server_description.try { |desc|
          description = SDAM::ServerDescription.new(desc.address)
          description.min_wire_version = desc.min_wire_version
          description.max_wire_version = desc.max_wire_version
          description.error = error.message
          description.last_update_time = desc.last_update_time
          topology.update(desc, description)
          close_connection_pool(desc) if error.shutdown?
          @monitors.find(&.server_description.address.== desc.address).try &.request_immediate_scan
        }
      end
    end

    if error.is_a?(Mongo::Error)
      if command.is_a? Commands::CommitTransaction
        error.add_unknown_transaction_label
      else
        error.add_transient_transaction_label
      end

      if error.transient_transaction? || error.unknown_transaction?
        session.try &.unpin
      end
    end

    raise error
  ensure
    release_connection(connection) if connection
    if result.is_a? Cursor
      # Bind the Cursor to the same server for its lifetime.
      result.server_description = server_description
      # Bind the session
      result.session = session
    else
      # End the session if implicit
      session.try &.end if session.try(&.implicit?)
    end
  end

  private def server_selection(command, args, read_preference : ReadPreference) : SDAM::ServerDescription
    # See: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#multi-threaded-or-asynchronous-server-selection
    selection_start_time = Time.utc
    selection_timeout = selection_start_time + @options.server_selection_timeout

    loop do
      unless topology.compatible
        raise Error::ServerSelection.new topology.compatibility_error
      end

      # Find suitable servers by topology type and operation type
      # Filter the suitable servers by calling the optional, application-provided server selector.
      # If there are any suitable servers, choose one at random from those within the latency window and return it;  otherwise, continue to the next step
      suitable_servers = find_suitable_servers(command, args, read_preference)
      selected_server = suitable_servers.try { |s| select_by_latency(s) }
      return selected_server if selected_server

      # Request an immediate topology check, then block the server selection thread until the topology changes or until the server selection timeout has elapsed
      @monitors.each { |monitor|
        monitor.request_immediate_scan
      }

      select
      when @topology_update.receive
      when timeout selection_timeout - Time.utc
      end

      # If more than serverSelectionTimeoutMS milliseconds have elapsed since the selection start time, raise a server selection error
      if Time.utc > selection_timeout
        raise Error::ServerSelection.new "Timeout (#{@options.server_selection_timeout}) reached while trying to select a suitable server with read preference #{read_preference.mode}."
      end
    end
  end

  # See: https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#executing-retryable-write-commands
  private def execute_retryable_write(
    command,
    session : Session::ClientSession,
    read_preference : ReadPreference,
    server_description : SDAM::ServerDescription? = nil,
    operation_id : Int64? = nil,
    **args
  )
    server_description ||= server_selection(command, args, read_preference)
    connection = get_connection(server_description)
    session.pin(server_description)

    if !topology.supports_sessions? || !server_description.supports_retryable_writes?
      return execute_command(command, session, read_preference, server_description, connection, operation_id, **args)
    end

    session.increment_txn_number unless session.is_transaction?

    begin
      return execute_command(command, session, read_preference, server_description, connection, operation_id, **args) { |body|
        if topology.supports_sessions?
          # txnNumber has been added to the body earlier if this is a transaction
          body["txnNumber"] = session.txn_number unless session.is_transaction?
        end

        # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#committransaction
        # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#majority-write-concern-is-used-when-retrying-committransaction
        if command.is_a?(Commands::CommitTransaction) && session.transitions_from.try &.committed?
          write_concern = body["writeConcern"]?
          write_concern = write_concern ? WriteConcern.from_bson(write_concern.as(BSON)) : WriteConcern.new
          write_concern.w = "majority"
          write_concern.w_timeout ||= 10_000
          body = body.copy_with({writeConcern: write_concern})
        end

        body
      }
    rescue error : Mongo::Error
      error.add_retryable_label(server_description.max_wire_version)
      error.add_unknown_transaction_label if error.retryable_write?

      if error.is_a?(Mongo::Error::Command) && error.code == 20 && error.message.try &.starts_with? "Transaction numbers"
        raise error
      elsif error.retryable_write?
        original_error = error
      else
        raise error
      end
    end

    begin
      server_description = session.server_description || server_selection(command, args, read_preference)
      connection = get_connection(server_description)
      session.pin(server_description)
    rescue
      raise original_error.not_nil!
    end

    if !topology.supports_sessions? || !server_description.supports_retryable_writes?
      raise original_error.not_nil!
    end

    begin
      execute_command(command, session, read_preference, server_description, connection, operation_id, **args) { |body|
        if topology.supports_sessions?
          # txnNumber has been added to the body earlier if this is a transaction
          body["txnNumber"] = session.txn_number unless session.is_transaction?
        end

        # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#committransaction
        # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#majority-write-concern-is-used-when-retrying-committransaction
        if command.is_a?(Commands::CommitTransaction)
          write_concern = body["writeConcern"]?
          write_concern = write_concern ? WriteConcern.from_bson(write_concern.as(BSON)) : WriteConcern.new
          write_concern.w = "majority"
          write_concern.w_timeout ||= 10_000
          body = body.copy_with({writeConcern: write_concern})
        end

        body
      }
    rescue error : Mongo::Error
      error.add_retryable_label(server_description.max_wire_version)
      error.add_unknown_transaction_label if error.retryable_write?

      raise error
    end
  end

  # See: https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst#implementing-retryable-reads
  private def execute_retryable_read(
    command,
    session : Session::ClientSession,
    read_preference : ReadPreference,
    server_description : SDAM::ServerDescription? = nil,
    operation_id : Int64? = nil,
    **args
  )
    server_description ||= server_selection(command, args, read_preference)
    connection = get_connection(server_description)
    session.pin(server_description)

    if !topology.supports_sessions? || !server_description.supports_retryable_reads?
      return execute_command(command, session, read_preference, server_description, connection, operation_id, **args)
    end

    begin
      return execute_command(command, session, read_preference, server_description, connection, operation_id, **args)
    rescue error : NetworkError
      error = Error::Network.new(error)
      original_error = error
    rescue error : Mongo::Error::Network
      original_error = error
    rescue error : Mongo::Error::Command
      if error.retryable_read?
        original_error = error
      else
        raise error
      end
    end

    begin
      server_description = session.server_description || server_selection(command, args, read_preference)
      connection = get_connection(server_description)
      session.pin(server_description)
    rescue
      raise original_error.not_nil!
    end

    if !topology.supports_sessions? || !server_description.supports_retryable_reads?
      raise original_error.not_nil!
    end

    begin
      execute_command(command, session, read_preference, server_description, connection, operation_id, **args)
    rescue error : Mongo::Error
      raise error
    end
  end

  protected def get_connection(server_description : SDAM::ServerDescription) : Mongo::Connection
    @pools[server_description.address] ||= DB::Pool(Mongo::Connection).new(
      initial_pool_size: @options.min_pool_size,
      max_pool_size: @options.max_pool_size,
      max_idle_pool_size: @options.max_pool_size,
      checkout_timeout: @options.wait_queue_timeout.try(&.seconds.to_f64) || 5.0
    ) do
      connection = Mongo::Connection.new(server_description, @credentials, @options)
      result, round_trip_time = connection.handshake(send_metadata: true, appname: @options.appname)
      connection.authenticate
      new_rtt = Connection.average_round_trip_time(round_trip_time, server_description.round_trip_time)
      new_description = SDAM::ServerDescription.new(server_description.address, result, new_rtt)
      topology.update(server_description, new_description)
      server_description.update(new_description)
      connection
    rescue e
      connection.try &.close
      raise e
    end
    @pools[server_description.address].checkout
  end

  private def release_connection(connection : Mongo::Connection)
    @@lock.synchronize {
      @pools[connection.server_description.address]?.try &.release(connection)
    }
  end

  protected def close_connection_pool(server_description : SDAM::ServerDescription)
    @@lock.synchronize {
      @pools[server_description.address]?.try &.close
      @pools.delete server_description.address
    }
  end

  protected def stop_monitoring(server_description : SDAM::ServerDescription)
    @@lock.synchronize {
      @monitors.reject!(server_description)
    }
  end

  protected def on_topology_update
    loop do
      select
      when @topology_update.send nil
        # Fiber.yield
      else
        break
      end
    end

    @@lock.synchronize {
      self.topology.servers.each { |server|
        no_monitor = @monitors.none? { |monitor|
          monitor.server_description.address.== server.address
        }
        add_monitor(server) if no_monitor
      }
    }
  end

  private def find_suitable_servers(command, args, read_preference : ReadPreference) : Array(SDAM::ServerDescription)?
    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#topology-type-unknown
    case self.topology.type
    when .unknown?
      nil
    when .single?
      self.topology.servers
    when .replica_set_no_primary?, .replica_set_with_primary?
      if WithReadPreference.must_use_primary_command?(command, args)
        if self.topology.type.replica_set_with_primary?
          select_primary
        else
          nil
        end
      else
        if read_preference.mode == "primary"
          select_primary
        elsif read_preference.mode == "secondary" || read_preference.mode == "nearest"
          servers = select_secondaries
          if read_preference.mode == "nearest"
            servers += select_primary
          end
          servers = filter_by_staleness(servers, read_preference)
          filter_by_tags(servers, read_preference)
        elsif read_preference.mode == "secondaryPreferred"
          result = find_suitable_servers(command, args, ReadPreference.new(mode: "secondary"))
          unless result.try &.size.try &.> 0
            return select_primary
          end
          result
        elsif read_preference.mode == "primaryPreferred"
          result = select_primary
          unless result.try &.size > 0
            return find_suitable_servers(command, args, ReadPreference.new(mode: "secondary"))
          end
          result
        end
      end
    when .sharded?
      self.topology.servers.select &.type.mongos?
    end
  end

  private def select_primary
    self.topology.servers.select &.type.rs_primary?
  end

  private def select_secondaries
    self.topology.servers.select &.type.rs_secondary?
  end

  private def filter_by_staleness(server_descriptions, read_preference) : Array(SDAM::ServerDescription)?
    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#maxstalenessseconds
    max_staleness = (read_preference.max_staleness_seconds || -1).seconds
    return server_descriptions unless max_staleness >= 0.seconds
    server_descriptions.select { |server|
      next true unless server.type.rs_secondary?
      if self.topology.type.replica_set_with_primary?
        primary = select_primary[0]
        staleness = (server.last_update_time - server.last_write_date.not_nil!) - (primary.last_update_time - primary.last_write_date.not_nil!) + @options.heartbeat_frequency
      else
        max_write_date = select_secondaries.max_of &.last_write_date.not_nil!
        staleness = max_write_date - server.last_write_date.not_nil! + @options.heartbeat_frequency
      end
      staleness <= max_staleness
    }
  end

  private def filter_by_tags(server_descriptions, read_preference) : Array(SDAM::ServerDescription)?
    # https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#tag-sets
    return server_descriptions unless (tag_sets = read_preference.tags) && tag_sets.size > 0
    server_descriptions.select { |server|
      tag_sets.any? { |tags|
        tags.all? { |key, value|
          server.tags.try &.[key].== value
        }
      }
    }
  end

  private def select_by_latency(server_descriptions) : SDAM::ServerDescription?
    # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#selecting-servers-within-the-latency-window
    return server_descriptions[0]? if server_descriptions.size < 2

    min_round_trip_time = server_descriptions.min_of &.round_trip_time
    eligible = server_descriptions.select { |server|
      server.round_trip_time - min_round_trip_time < @options.local_threshold
    }
    eligible.sample(1)[0]
  end

  private def start_monitoring
    @monitors.each { spawn &.scan }
  end

  protected def add_monitor(server_description : SDAM::ServerDescription, *, start_monitoring = true)
    monitor = SDAM::Monitor.new(self, server_description, @credentials, @options.heartbeat_frequency || 10.seconds)
    @monitors << monitor
    if start_monitoring
      spawn monitor.scan
    end
  end

  private def gossip_cluster_time(session : Session::ClientSession? = nil)
    # see: https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#gossipping-the-cluster-time
    if session
      client_time = @cluster_time
      session_time = session.cluster_time
      if !client_time || (session_time && client_time < session_time)
        session_time
      else
        client_time
      end
    else
      @cluster_time
    end
  end

  # :nodoc:
  UNACKNOWLEDGED_WRITE_PROHIBITED_OPTIONS = {
    "hint",
    "collation",
    "bypass_document_validation",
    "array_filters",
  }

  private def acknowledged?(args, session, validate = true)
    unacknowledged = false
    if concern = args["options"]?.try(&.["write_concern"]?)
      unacknowledged = concern.unacknowledged?
    end

    if unacknowledged && validate
      if session.is_transaction?
        raise Error::Transaction.new("Transactions do not support unacknowledged write concerns.")
      end

      prohibited_option = nil
      UNACKNOWLEDGED_WRITE_PROHIBITED_OPTIONS.each { |option|
        if args["options"]?.try { |item| item.has_key?(option) && !item[option]?.nil? }
          prohibited_option = option
          break
        elsif args["updates"]?.try(&.any? { |item| item.has_key?(option) && !item[option]?.nil? })
          prohibited_option = option
          break
        elsif args["deletes"]?.try(&.any? { |item| item.has_key?(option) && !item[option]?.nil? })
          prohibited_option = option
          break
        end
      }
      raise Mongo::Error.new("Option #{prohibited_option} is prohibited when performing an unacknowledged write.") if prohibited_option
    end

    !unacknowledged
  end
end
