module Mongo
  # Collation allows users to specify language-specific rules for string comparison, such as rules for lettercase and accent marks.
  #
  # See: [the official documentation](https://docs.mongodb.com/manual/reference/collation/index.html)
  @[BSON::Options(camelize: "lower")]
  struct Collation
    include BSON::Serializable

    # The ICU locale.
    property locale : String
    # Flag that determines whether to include case comparison at strength level 1 or 2.
    property case_level : Bool? = nil
    # A field that determines sort order of case differences during tertiary level comparisons.
    property case_first : String? = nil
    # The level of comparison to perform.
    property strength : Int32? = nil
    # Flag that determines whether to compare numeric strings as numbers or as strings.
    property numeric_ordering : Bool? = nil
    # Field that determines whether collation should consider whitespace and punctuation as base characters for purposes of comparison.
    property alternate : String? = nil
    # Field that determines up to which characters are considered ignorable when `alternate: "shifted"`. Has no effect if `alternate: "non-ignorable"`
    property max_variable : String? = nil
    #  Flag that determines whether strings with diacritics sort from back of the string, such as with some French dictionary ordering.
    property backwards : Bool? = nil
    # Flag that determines whether to check if text require normalization and to perform normalization.
    # Generally, majority of text does not require this normalization processing.
    property normalization : Bool? = nil

    # Create a new `Collation` instance.
    #
    # ```
    # collation = Mongo::Collation.new(locale: "fr")
    # ```
    def initialize(
      @locale,
      @case_level = nil,
      @case_first = nil,
      @strength = nil,
      numeric_ordering = nil,
      alternate = nil,
      max_variable = nil,
      backwards = nil,
      normalization = nil
    )
    end
  end
end
