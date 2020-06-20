# :nodoc:
record Mongo::Credentials,
  username : String? = nil,
  password : String? = nil,
  source : String? = "admin",
  mechanism : String? = nil,
  mechanism_properties : String? = nil
