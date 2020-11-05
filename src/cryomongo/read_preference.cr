require "bson"

module Mongo
  # The read preference describes how MongoDB clients route read operations to the members of a replica set.
  #
  # See: [the official documentation](https://docs.mongodb.com/manual/core/read-preference/index.html).
  @[BSON::Options(camelize: "lower")]
  record ReadPreference,
    mode : String,
    tags : Array(BSON)? = nil,
    max_staleness_seconds : Int32? = nil,
    hedge : BSON? = nil {
    include BSON::Serializable
  }

  private module WithReadPreference
    macro included
      # ReadPreference accessor.
      #
      # See: [the official documentation](https://docs.mongodb.com/manual/core/read-preference/index.html).
      property read_preference : ReadPreference? = nil
    end

    protected def self.must_use_primary_command?(command, command_args)
      !command.is_a?(Commands::MayUseSecondary) || !command.may_use_secondary?(**command_args)
    end

    private def self.mix(args, read_preference)
      if (options = args["options"]?)
        if options["read_preference"]?
          args
        else
          args.merge({
            options: options.merge({
              read_preference: read_preference,
            }),
          })
        end
      else
        args
      end
    end

    protected def self.mix_read_preference(command, args, read_preference : ReadPreference?, topology : SDAM::TopologyDescription, server_description : SDAM::ServerDescription)
      case topology.type
      when .single?
        # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#topology-type-single
        case server_description.type
        when .mongos?
          self.mix(args, read_preference)
        when .standalone?
          args
        else
          if read_preference.try &.mode != "primary"
            args
          else
            self.mix(args, ReadPreference.new(mode: "primaryPreferred"))
          end
        end
        # when .sharded?
      else
        # see: https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#topology-type-sharded
        self.mix(args, read_preference)
        # else
        #   args
      end
    end
  end
end
