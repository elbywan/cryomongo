require "bson"

module Mongo
  # The common error ancestor class.
  class Error < Exception
    getter error_labels : Set(String) = Set(String).new

    def add_error_label(label : String) : Nil
      @error_labels << label
    end

    def has_error_label?(label : String) : Bool
      error_labels.includes?(label)
    end

    def retryable_write?
      has_error_label?("RetryableWriteError")
    end

    def retryable_read?
      self.is_a?(Error::Network) || (
        self.is_a?(Error::Command) && self.retryable_code?
      ) || (
        self.is_a?(Error::CommandWrite) && self.errors.any?(&.retryable_code?)
      )
    end

    def transient_transaction?
      has_error_label?("TransientTransactionError")
    end

    def unknown_transaction?
      has_error_label?("UnknownTransactionCommitResult")
    end

    def add_retryable_label(wire_version : Int32)
      add_label = begin
        if self.is_a?(Error::Network)
          true
        elsif wire_version < 9
          if self.is_a?(Error::Command)
            self.retryable_code?
          elsif self.is_a?(Error::CommandWrite)
            self.errors.any?(&.retryable_code?)
          end
        end
      end

      add_error_label("RetryableWriteError") if add_label
    end

    def add_transient_transaction_label
      # "in the case of network errors or server selection errors where the client receives no server reply, the client adds the label"
      # https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#transienttransactionerror
      if self.is_a?(Error::ServerSelection) || self.is_a?(Error::Network)
        add_error_label("TransientTransactionError")
      end
    end

    def add_unknown_transaction_label
      if self.is_a?(Mongo::Error) && self.retryable_write?
        self.add_error_label("UnknownTransactionCommitResult")
      elsif self.is_a?(Mongo::Error::Command) && self.max_time_ms_expired?
        self.add_error_label("UnknownTransactionCommitResult")
      elsif self.is_a?(Error::WriteConcern)
        if self.max_time_ms_expired? || self.shutdown_in_progress? || self.failed_or_timeout?
          self.add_error_label("UnknownTransactionCommitResult")
        end
      elsif self.is_a?(Error::Client) || self.is_a?(Error::ServerSelection)
        self.add_error_label("UnknownTransactionCommitResult")
      end
    end
  end

  class Error::Server < Error
  end

  class Error::Client < Error
  end

  class Error::Network < Error::Client
    def initialize(original_error : IO::Error | Socket::Error)
      initialize(message: original_error.message, cause: original_error)
    end
  end

  # class Error::Handshake < Error
  # end

  # class Error::Connection < Error
  # end

  # Is raised during server selection when encountering a timeout or a compatibility issue.
  class Error::ServerSelection < Error
  end

  # Is raised when the server replies with an error to a command request.
  class Error::Command < Error::Server
    getter code : Int32
    getter code_name : String?

    # See: https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#not-master-and-node-is-recovering
    RECOVERING_CODES    = {11600, 11602, 13436, 189, 91}
    RECOVERING_MESSAGES = {"not master or secondary", "node is recovering"}
    NOT_MASTER_CODES    = {10107, 13435}
    NOT_MASTER_MESSAGES = {"not master"}
    SHUTDOWN_CODES      = {11600, 91}
    # See: https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.rst#determining-retryable-errors
    RETRYABLE_CODES = {6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436}
    # See: https://github.com/mongodb/specifications/blob/f1fcb6aa9751e5ed7eb8e64c0f08f1edf10a859a/source/change-streams/change-streams.rst#resumable-error
    RESUMABLE_CODES = {63, 133, 150, 234, 13388, 133} + RETRYABLE_CODES

    def initialize(code, @code_name, message, *, @error_labels = Set(String).new)
      @code = code.try &.as(Int32) || 0
      @message = message.try(&.as(String)) || ""
    end

    def to_s(io : IO)
      io << "Code: #{@code} - #{@message}"
    end

    def recovering?
      @code.in?(RECOVERING_CODES) ||
        RECOVERING_MESSAGES.any? &.in? @message.not_nil!
    end

    def not_master?
      @code.in?(NOT_MASTER_CODES) ||
        self.recovering? ||
        NOT_MASTER_MESSAGES.any? &.in? @message.not_nil!
    end

    def shutdown?
      @code.in? SHUTDOWN_CODES
    end

    def state_change?
      recovering? || not_master?
    end

    def retryable_code?
      @code.in?(RETRYABLE_CODES)
    end

    def resumable?
      @code.in? RESUMABLE_CODES
    end

    def max_time_ms_expired?
      @code == 50
    end
  end

  # Is raised when the server replies to a write with one or more WriteErrors.
  class Error::CommandWrite < Error::Server
    getter errors = [] of Error::Command

    def initialize(errors : BSON)
      errors.each { |_, error|
        error = error.as(BSON)
        err_code = error["code"]?
        err_code_name = error["codeName"]?.try &.as(String)
        err_msg = error["errmsg"]?.try &.as(String)
        err_labels = error["errorLabels"]?.try { |labels| Array(String).from_bson(labels) } || [] of String
        @errors << Error::Command.new(err_code, err_code_name, err_msg, error_labels: Set(String).new(err_labels))
      }
    end

    def message
      @errors.join('\n')
    end
  end

  # Is raised when the server replies to a write with a WriteConcernError.
  class Error::WriteConcern < Error::Command
    getter details : BSON?

    def initialize(error : BSON)
      @code = error["code"]?.try(&.as(Int).to_i32) || 0
      @message = error["errmsg"]?.try(&.as(String)) || ""
      @details = error["err_info"]?.try &.as(BSON)
    end

    def failed_or_timeout?
      @code == 64
    end

    def shutdown_in_progress?
      @code == 91
    end

    def unsatisfiable?
      @code == 100
    end

    def max_time_ms_expired?
      @code == 50
    end

    def unknown_repl?
      @code == 79
    end
  end

  # Is raised when performing transaction operations.
  class Error::Transaction < Error::Client
  end
end
