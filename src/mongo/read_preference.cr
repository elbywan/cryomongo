require "bson"

module Mongo
  @[BSON::Options(camelize: "lower")]
  record ReadPreference,
    mode : String,
    tags : Array(BSON)? = nil,
    max_staleness_seconds : Int32? = nil,
    hedge : BSON? = nil {
    include BSON::Serializable
  }

  module WithReadPreference
    macro included
      property read_preference : ReadPreference? = nil
    end

    MAY_USE_SECONDARY = {
      # Commands::Aggregate, # without a write stage (e.g. $out, $merge)
      # Commands::CollStats,
      Commands::Count,
      # Commands::DbStats,
      Commands::Distinct,
      Commands::Find,
      # Commands::GeoNear,
      # Commands::GeoSearch,
      # Commands::Group,
      # Commands::MapReduce, # where the out option is { inline: 1 }
      # Commands:: ParallelCollectionScean
    }

    def self.must_use_primary_command?(command, command_args)
      !MAY_USE_SECONDARY.includes?(command) ||
        command == Commands::Aggregate && command_args["pipeline"]?.try { |pipeline|
          pipeline.as(Array).map { |elt| BSON.new(elt) }.any? { |stage|
            stage["$out"]? || stage["$merge"]?
          }
        }
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

    def self.mix_read_preference(command, args, read_preference : ReadPreference?, topology : SDAM::TopologyDescription, server_description : SDAM::ServerDescription)
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
