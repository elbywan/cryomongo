require "bson"

module Mongo
  # Write concern describes the level of acknowledgment requested from MongoDB for write operations to a standalone mongod or to replica sets or to sharded clusters.
  #
  # In sharded clusters, mongos instances will pass the write concern on to the shards.
  #
  # See: [the official documentation](https://docs.mongodb.com/manual/reference/write-concern/index.html)
  struct WriteConcern
    include BSON::Serializable

    # The *j* option requests acknowledgment from MongoDB that the write operation has been written to the on-disk journal.
    property j : Bool? = nil
    # The *w* option requests acknowledgment that the write operation has propagated to a specified number of mongod instances or to mongod instances with specified tags.
    property w : (Int32 | String)? = nil
    # This option specifies a time limit, in milliseconds, for the write concern. *wtimeout* is only applicable for w values greater than 1.
    @[BSON::Field(key: "wtimeout")]
    property w_timeout : Int64? = nil

    # Create a WriteConcern instance.
    def initialize(@j : Bool? = nil, @w : (Int32? | String)? = nil, @w_timeout : Int64? = nil)
      raise Mongo::Error.new "Invalid write concern" if @j == true && w == 0
    end

    def unacknowledged?
      @w == 0 && !@j
    end
  end

  # The readConcern option allows to control the consistency and isolation properties of the data read from replica sets and replica set shards.
  #
  # Through the effective use of write concerns and read concerns, you can adjust the level of consistency and availability guarantees as appropriate,
  # such as waiting for stronger consistency guarantees, or loosening consistency requirements to provide higher availability.
  #
  # See: [the official documentation](https://docs.mongodb.com/manual/reference/read-concern/index.html)
  @[BSON::Options(camelize: "lower")]
  struct ReadConcern
    include BSON::Serializable

    # The read concern level.
    property level : String? = nil
    # :nodoc:
    property after_cluster_time : BSON::Timestamp? = nil

    # Create a ReadConcern instance.
    def initialize(@level = nil, @after_cluster_time = nil)
    end
  end

  private module WithWriteConcern
    macro included
      # Write concern accessor.
      #
      # See: [the official documentation](https://docs.mongodb.com/manual/reference/write-concern/index.html)
      property write_concern : WriteConcern? = nil
    end

    protected def self.mix_write_concern(command, args, write_concern : WriteConcern?, *, session : Session::ClientSession)
      options = args["options"]?

      if options && session.is_transaction?
        if command.is_a?(Commands::CommitTransaction) || command.is_a?(Commands::AbortTransaction)
          write_concern = options["write_concern"]? || session.current_transaction_options.write_concern
          args.merge({
            options: options.merge({
              write_concern: write_concern,
            }),
          })
        elsif options["write_concern"]?
          raise Error::Transaction.new("Cannot set write concern after starting a transaction.")
        else
          args
        end
      elsif options && command.is_a?(Commands::WriteCommand) && command.write_command?(**args)
        if options["write_concern"]?
          args
        else
          args.merge({
            options: options.merge({
              write_concern: write_concern,
            }),
          })
        end
      else
        args
      end
    end
  end

  private module WithReadConcern
    macro included
      # Read concern accessor.
      #
      # See: [the official documentation](https://docs.mongodb.com/manual/reference/read-concern/index.html)
      property read_concern : ReadConcern? = nil
    end

    protected def self.mix_read_concern(command, args, read_concern : ReadConcern?, *, session : Session::ClientSession)
      options = args["options"]?

      if options && session.is_transaction?
        if options["read_concern"]?
          raise Error::Transaction.new("Cannot set read concern after starting a transaction.")
        end

        if session.transitions_from.try &.starting?
          concern = options["read_concern"]? || session.current_transaction_options.read_concern
          if session.options.causal_consistency
            # Drivers MUST add readConcern.afterClusterTime to the command that starts a transaction in a causally consistent session -- even if the command is a write.
            # https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#interaction-with-causal-consistency
            if after_cluster_time = session.operation_time
              concern ||= ReadConcern.new
              concern.after_cluster_time = after_cluster_time
            end
          end

          args.merge({
            options: options.merge({
              read_concern: concern,
            }),
          })
        else
          args
        end
      elsif options && command.is_a?(Commands::ReadCommand) && command.read_command?(**args)
        concern = options["read_concern"]? || read_concern
        after_cluster_time = session.operation_time if session.options.causal_consistency

        if after_cluster_time
          concern = concern || read_concern || ReadConcern.new
          concern.after_cluster_time = after_cluster_time
          args.merge({
            options: options.merge({
              read_concern: concern,
            }),
          })
        else
          args.merge({
            options: options.merge({
              read_concern: read_concern,
            }),
          })
        end
      else
        args
      end
    end
  end
end
