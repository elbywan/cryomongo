# :nodoc:
module Mongo::Index
  struct Model
    property keys : BSON
    property options : Options

    def initialize(keys, @options = Index::Options.new)
      @keys = BSON.new keys
    end
  end

  @[BSON::Options(camelize: "lower")]
  struct Options
    include Tools::Initializer
    include BSON::Serializable

    property background : Bool? = nil
    property expire_after_seconds : Int32? = nil
    property name : String? = nil
    property sparse : Bool? = nil
    property storage_engine : BSON? = nil
    property unique : Bool? = nil
    property version : Int32? = nil
    property default_language : String? = nil
    property language_override : String? = nil
    property text_index_version : Int32? = nil
    property weights : BSON? = nil
    @[BSON::Field(key: "2dsphereIndexVersion")]
    property _2dsphere_index_version : Int32? = nil
    property bits : Int32? = nil
    property max : Float64? = nil
    property min : Float64? = nil
    property bucket_size : Int32? = nil
    property partial_filter_expression : BSON? = nil
    property collation : Collation? = nil
    property wildcard_projection : BSON? = nil
    property hidden : Bool? = nil
    property commit_quorum : (Int32 | String)? = nil
    property max_time_ms : Int64? = nil
    property write_concern : WriteConcern? = nil
  end
end
