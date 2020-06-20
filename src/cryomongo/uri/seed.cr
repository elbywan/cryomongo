# :nodoc:
record Mongo::URI::Seed, host : String, port : Int32 = 27017 {
  def address
    if host.ends_with? ".sock"
      host
    else
      "#{host}:#{port}"
    end
  end
}
