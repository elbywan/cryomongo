require "../concerns"
require "../read_preference"

module Mongo::Session
  record TransactionOptions,
    # The readConcern to use for this transaction.
    read_concern : ReadConcern? = nil,
    # The writeConcern to use for this transaction.
    write_concern : WriteConcern? = nil,
    # The readPreference to use for this transaction.
    read_preference : ReadPreference? = nil,
    # The maximum amount of time to allow a single commitTransaction
    # command to run.
    max_commit_time_ms : Int64? = nil

  enum TransactionState
    None
    Starting
    InProgress
    Committed
    Aborted
  end

  enum TransactionStateEvent
    Insert
    Start
    Commit
    Abort
  end

  class ClientSession
    # The state of the transaction.
    getter transaction_state : TransactionState = :none
    # The state from where the transaction is transitioning.
    getter transitions_from : TransactionState? = nil
    # Options for the current transaction.
    getter current_transaction_options = TransactionOptions.new
    # Server description if the session is pinned to a specific mongos.
    getter server_description : SDAM::ServerDescription? = nil
    # The recoveryToken field enables the driver to recover a sharded transaction's outcome on a new mongos when the original mongos is no longer available.
    property recovery_token : BSON? = nil

    @transactions_lock = Mutex.new
    @empty_commit = false

    def is_transaction?
      !transaction_state.none?
    end

    # Starts a new transaction with the given options.
    #
    # This session's `options.default_transaction_options` of type `TransactionOptions` is used when options is omitted.
    #
    # NOTE: Raises an error if this session is already in a transaction.
    #
    # ```
    # client = Mongo::Client.new
    # session = client.start_session
    #
    # # transaction options arguments are optional
    # session.start_transaction(
    #   read_concern: Mongo::ReadConcern.new(level: "snapshot"),
    #   write_concern: Mongo::WriteConcern.new(w: "majority")
    # )
    # ```
    def start_transaction(**options)
      @current_transaction_options = (@options.default_transaction_options || TransactionOptions.new)
      @current_transaction_options = @current_transaction_options.copy_with(
        read_concern: options["read_concern"]? || @current_transaction_options.read_concern || @client.read_concern,
        write_concern: options["write_concern"]? || @current_transaction_options.write_concern || @client.write_concern,
        read_preference: options["read_preference"]? || @current_transaction_options.read_preference || @client.read_preference,
        max_commit_time_ms: options["max_commit_time_ms"]? || @current_transaction_options.max_commit_time_ms
      )

      if write_concern = @current_transaction_options.write_concern
        if write_concern.unacknowledged?
          raise Error::Transaction.new("Transactions do not support unacknowledged write concerns.")
        end
      end

      state_transition(:start) {
        increment_txn_number
        self.unpin
      }
    end

    # Same as `start_transaction` but will commit the transaction after the block returns.
    #
    # NOTE: If an error is thrown, the transaction will be aborted.
    #
    # ```
    # client = Mongo::Client.new
    # session = client.start_session
    # session.with_transaction {
    #   client["db"]["collection"].tap { |collection|
    #     collection.insert_one({_id: 1}, session: session)
    #     collection.insert_one({_id: 2}, session: session)
    #   }
    # }
    # ```
    def with_transaction(**options, &block)
      start_transaction(**options)
      yield
      commit_transaction
    rescue e
      abort_transaction
      raise e
    end

    # Commits the currently active transaction in this session.
    #
    # NOTE: Raises an error if this session has no transaction.
    #
    # ```
    # client = Mongo::Client.new
    # session = client.start_session
    # session.start_transaction
    # client["db"]["collection"].tap { |collection|
    #   collection.insert_one({_id: 1}, session: session)
    #   collection.insert_one({_id: 2}, session: session)
    # }
    # session.commit_transaction
    # ```
    def commit_transaction(*, write_concern : WriteConcern? = nil)
      state_transition(:commit, rollback_status_on_error: false) {
        skip_commit = @transitions_from.try(&.starting?) || false
        @client.command(
          Commands::CommitTransaction,
          session: self,
          options: {
            write_concern:  write_concern,
            max_time_ms:    current_transaction_options.max_commit_time_ms,
            recovery_token: recovery_token,
          }
        ) unless @empty_commit || skip_commit
        @empty_commit = skip_commit
      }
    end

    # Aborts the currently active transaction in this session.
    #
    # NOTE: Raises an error if this session has no transaction.
    #
    # ```
    # client = Mongo::Client.new
    # session = client.start_session
    # session.start_transaction
    # client["db"]["collection"].tap { |collection|
    #   collection.insert_one({_id: 1}, session: session)
    #   collection.insert_one({_id: 2}, session: session)
    # }
    # session.abort_transaction
    # ```
    def abort_transaction(*, write_concern : WriteConcern? = nil)
      state_transition(:abort, rollback_status_on_error: false) {
        @client.command(
          Commands::AbortTransaction,
          session: self,
          options: {
            write_concern:  write_concern,
            recovery_token: recovery_token,
          }
        ) unless @transitions_from.try &.starting?
        self.unpin
      }
    end

    # Aborts any currently active transaction and ends this session.
    def end
      # see: https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#endsession-changes
      begin
        # Drivers MUST call abortTransaction if this session is in the "transaction in progress" state in order to release resources on the server.
        abort_transaction if @transaction_state.in_progress?
      rescue
        # Drivers MUST ignore any errors raised by abortTransaction while ending a session.
      end
      previous_def
    end

    protected def insert_transaction
      state_transition(:insert) {
        yield
      }
    end

    protected def pin(server_description : SDAM::ServerDescription) : Nil
      if is_transaction? && server_description.type.mongos?
        @server_description = server_description
      end
    end

    protected def unpin : Nil
      @server_description = nil
    end

    # -- Private

    private def state_transition(event : TransactionStateEvent, *, rollback_status_on_error = true, &block)
      @transactions_lock.synchronize do
        @transitions_from = @transaction_state
        @transaction_state = begin
          case {@transaction_state, event}
          when {.none?, .insert?}, {.committed?, .insert?}, {.aborted?, .insert?}
            @recovery_token = nil
            TransactionState::None
          when {.none?, .start?}, {.committed?, .start?}, {.aborted?, .start?}
            @recovery_token = nil
            TransactionState::Starting
          when {.starting?, .insert?}, {.in_progress?, .insert?}
            TransactionState::InProgress
          when {.starting?, .commit?}, {.in_progress?, .commit?}, {.committed?, .commit?}
            TransactionState::Committed
          when {.starting?, .abort?}, {.in_progress?, .abort?}
            TransactionState::Aborted
          when {.starting?, .start?}, {.in_progress?, .start?}
            raise Error::Transaction.new("Transaction already in progress.")
          when {.none?, .commit?}, {.none?, .abort?}
            raise Error::Transaction.new("No transaction started.")
          when {.aborted?, .commit?}
            raise Error::Transaction.new("Cannot call commitTransaction after calling abortTransaction.")
          when {.committed?, .abort?}
            raise Error::Transaction.new("Cannot call abortTransaction after calling commitTransaction.")
          when {.aborted?, .abort?}
            raise Error::Transaction.new("Cannot call abortTransaction twice.")
          else
            raise Error::Transaction.new("Cannot perform event '#{event}' when the transaction is in '#{@transaction_state}' state.")
          end
        end
        yield
      end
      # Do not restore previous state for server or network errors
    rescue e : Error::Server
      raise e
    rescue e : Error::Network
      raise e
    rescue e
      if rollback_status_on_error
        # Restore previous state
        @transactions_lock.synchronize { @transaction_state = @transitions_from.not_nil! }
      end
      raise e
    ensure
      @transactions_lock.synchronize { @transitions_from = nil }
    end
  end
end
