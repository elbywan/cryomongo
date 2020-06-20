require "../tools"

# This module contains the [Database Commands](https://docs.mongodb.com/manual/reference/command/) supported by the `cryomongo` driver.
module Mongo::Commands
  # Common results.
  module Common
    # :nodoc:
    module Result
      macro included
        property ok : Float64
        property operation_time : BSON::Timestamp?
        @[BSON::Field(key: "$clusterTime")]
        property cluster_time : BSON?
      end
    end

    # :nodoc:
    macro result(name, root = true, &block)
      @[BSON::Options(camelize: "lower")]
      struct {{name.id}}
        include BSON::Serializable
        {% if root %}include ::Mongo::Commands::Common::Result{% end %}

        {{ yield }}
      end
    end

    # A Base MongoDB result.
    result(BaseResult)

    # WriteError bson sub-document.
    result(WriteError, root: false) {
      property index : Int32
      property code : Int32
      property errmsg : String

      def initialize(@index, @code, @errmsg); end
    }

    # WriteConcernError bson sub-document.
    result(WriteConcernError, root: false) {
      property code : Int32
      property errmsg : String
    }

    # Cursor bson sub-document.
    result(Cursor, root: false) {
      property first_batch : Array(BSON)
      property id : Int64
      property ns : String
    }

    # Upserted bson sub-document.
    result(Upserted, root: false) {
      property index : Int32
      property _id : BSON::Value

      def initialize(@index, @_id); end
    }

    # In response to query commands.
    result(QueryResult) {
      property cursor : Cursor
    }

    # In response to insert commands.
    result(InsertResult) {
      property n : Int32?
      property write_errors : Array(WriteError)?
      property write_concern_error : WriteConcernError?
    }

    # In response to delete commands.
    result(DeleteResult) {
      property n : Int32?
      property write_errors : Array(WriteError)?
      property write_concern_error : WriteConcernError?
    }

    # In response to update commands.
    result(UpdateResult) {
      property n : Int32?
      property n_modified : Int32?
      property upserted : Array(Upserted)?
      property write_errors : Array(WriteError)?
      property write_concern_error : WriteConcernError?
    }

    # In response to findAndModify commands.
    result(FindAndModifyResult) {
      property value : BSON?
      property last_error_object : BSON?
    }
  end

  # :nodoc:
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

  # :nodoc:
  def self.make(init, options = nil, sequences = nil, skip_nil = true)
    self.make(init, options, sequences, skip_nil) { false }
  end
end
