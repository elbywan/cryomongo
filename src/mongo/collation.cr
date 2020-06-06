module Mongo
  @[BSON::Options(camelize: "lower")]
  record Collation,
    locale : String,
    case_level : Bool? = nil,
    case_first : String? = nil,
    strength : Int32? = nil,
    numeric_ordering : Bool? = nil,
    alternate : String? = nil,
    max_variable : String? = nil,
    backwards : Bool? = nil {
    include BSON::Serializable
  }
end
