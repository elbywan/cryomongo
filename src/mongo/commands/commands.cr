require "../tools"

module Mongo::Commands
  module Common
    module Result
      macro included
        property ok : Float64
        property operation_time : BSON::Timestamp?
        @[BSON::Field(key: "$clusterTime")]
        property cluster_time : BSON?
      end
    end

    macro result(name, root = true, &block)
      @[BSON::Options(camelize: "lower")]
      struct {{name.id}}
        include BSON::Serializable
        {% if root %}include ::Mongo::Commands::Common::Result{% end %}

        {{ yield }}
      end
    end

    result(BaseResult)

    result(WriteError, root: false) {
      property index : Int32
      property code : Int32
      property errmsg : String

      def initialize(@index, @code, @errmsg); end
    }

    result(WriteConcernError, root: false) {
      property code : Int32
      property errmsg : String
    }

    result(Cursor, root: false) {
      property first_batch : Array(BSON)
      property id : Int64
      property ns : String
    }

    result(Upserted, root: false) {
      property index : Int32
      property _id : BSON::Value

      def initialize(@index, @_id); end
    }

    result(QueryResult) {
      property cursor : Cursor
    }

    result(InsertResult) {
      property n : Int32?
      property write_errors : Array(WriteError)?
      property write_concern_error : WriteConcernError?
    }

    result(DeleteResult) {
      property n : Int32?
      property write_errors : Array(WriteError)?
      property write_concern_error : WriteConcernError?
    }

    result(UpdateResult) {
      property n : Int32?
      property n_modified : Int32?
      property upserted : Array(Upserted)?
      property write_errors : Array(WriteError)?
      property write_concern_error : WriteConcernError?
    }

    result(FindAndModifyResult) {
      property value : BSON?
      property last_error_object : BSON?
    }
  end

  def self.make(init, options = nil, sequences = nil, skip_nil = true)
    bson = BSON.new(init)
    options.try &.each { |key, value|
      skip_key = yield bson, key, value
      if skip_key == false && (skip_nil == false || !value.nil?)
        if key.to_s == "read_preference"
          bson["$readPreference"] = value
        elsif key.to_s == "max_time_ms"
          bson["maxTimeMS"] = value
        else
          bson[key.to_s.camelcase(lower: true)] = value
        end
      end
    }
    bson
    {bson, sequences}
  end

  def self.make(init, options = nil, sequences = nil, skip_nil = true)
    self.make(init, options, sequences, skip_nil) { false }
  end
end
