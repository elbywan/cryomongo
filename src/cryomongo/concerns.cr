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
    property wtimeout : Int64? = nil

    # Create a WriteConcern instance.
    def initialize(@j = nil, @w = nil, @wtimeout = nil)
      raise Mongo::Error.new "Invalid write concern" if @j == true && w == 0
    end
  end

  # The readConcern option allows to control the consistency and isolation properties of the data read from replica sets and replica set shards.
  #
  # Through the effective use of write concerns and read concerns, you can adjust the level of consistency and availability guarantees as appropriate,
  # such as waiting for stronger consistency guarantees, or loosening consistency requirements to provide higher availability.
  #
  # See: [the official documentation](https://docs.mongodb.com/manual/reference/read-concern/index.html)
  struct ReadConcern
    include BSON::Serializable

    # The read concern level.
    property level : String? = nil

    # Create a ReadConcern instance.
    def initialize(@level = nil)
    end
  end

  private module WithWriteConcern
    macro included
      # Write concern accessor.
      #
      # See: [the official documentation](https://docs.mongodb.com/manual/reference/write-concern/index.html)
      property write_concern : WriteConcern? = nil
    end

    WRITE_COMMANDS = {
      Commands::Aggregate,
      Commands::Insert,
      Commands::Update,
      Commands::Delete,
      Commands::FindAndModify,
      Commands::Create,
      Commands::CreateIndexes,
      Commands::Drop,
      Commands::DropDatabase,
      Commands::DropIndexes,
      # Commands::CreateUser,
      # Commands::UpdateUser,
      # Commands::DropUser,
      # Commands::MapReduce,
      # Commands::CopyDb,
      # Commands::Clone,
      # Commands::CloneCollection,
    }

    protected def self.mix_write_concern(command, args, write_concern : WriteConcern?)
      if (options = args["options"]?) && WRITE_COMMANDS.includes?(command)
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

    READ_COMMANDS = {
      Commands::Aggregate,
      Commands::FindAndModify,
      Commands::Count,
      Commands::Distinct,
      Commands::Find,
    }

    protected def self.mix_read_concern(command, args, read_concern : ReadConcern?)
      if (options = args["options"]?) && READ_COMMANDS.includes?(command)
        if options["read_concern"]?
          args
        else
          args.merge({
            options: options.merge({
              read_concern: read_concern.try(&.to_bson),
            }),
          })
        end
      else
        args
      end
    end
  end
end
