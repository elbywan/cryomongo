module Mongo
  @[BSON::Options(camelize: "lower")]
  struct Collation
    include BSON::Serializable

    property locale : String
    property case_level : Bool? = nil
    property case_first : String? = nil
    property strength : Int32? = nil
    property numeric_ordering : Bool? = nil
    property alternate : String? = nil
    property max_variable : String? = nil
    property backwards : Bool? = nil

    def initialize(
      @locale,
      @case_level = nil,
      @case_first = nil,
      @strength = nil,
      numeric_ordering = nil,
      alternate = nil,
      max_variable = nil,
      backwards = nil,
    )
    end
  end
end
