require "bson"

module Mongo
  record WriteConcern,
    j : Bool? = nil,
    w : (Int32 | String)? = nil,
    wtimeout : Int64? = nil {
    include BSON::Serializable

    def initialize(@j, @w, @wtimeout)
      raise "Invalid write concern" if @j == true && w == 0
    end
  }

  record ReadConcern,
    level : String? = nil {
      include BSON::Serializable
    }

  module WithWriteConcern
    macro included
      property write_concern : WriteConcern? = nil
    end

    WRITE_COMMANDS = {
      Commands::Aggregate,
      Commands::Insert,
      Commands::Update,
      Commands::Delete,
      Commands::FindAndModify,
      # Commands::CopyDb,
      # Commands::Create,
      # Commands::CreateIndexes,
      # Commands::Drop,
      # Commands::DropDatabase,
      # Commands::DropIndexes,
      # Commands::MapReduce,
      # Commands::Clone,
      # Commands::CloneCollection,
      # Commands::CreateUser,
      # Commands::UpdateUser,
      # Commands::DropUser,
    }

    def self.mix_write_concern(command, args, write_concern : WriteConcern?)
      if (options = args["options"]?) && WRITE_COMMANDS.includes?(command)
        if options["write_concern"]?
          args
        else
          args.merge({
            options: options.merge({
              write_concern: write_concern
            })
          })
        end
      else
        args
      end
    end
  end

  module WithReadConcern
    macro included
      property read_concern : ReadConcern? = nil
    end

    READ_COMMANDS = {
      Commands::Aggregate,
      Commands::FindAndModify,
      Commands::Count,
      Commands::Distinct,
      Commands::Find,
    }

    def self.mix_read_concern(command, args, read_concern : ReadConcern?)
      if (options = args["options"]?) && READ_COMMANDS.includes?(command)
        if options["read_concern"]?
          args
        else
          args.merge({
            options: options.merge({
              read_concern: read_concern.try(&.to_bson)
            })
          })
        end
      else
        args
      end
    end
  end
end
