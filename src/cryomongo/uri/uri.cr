require "uri"
require "./seed"
require "./options"
require "../connection/*"

# :nodoc:
module Mongo::URI
  extend self

  FORBIDDEN_DATABASE_CHARACTERS = {
    '/',
    '\\',
    ' ',
    '"',
    "$",
  }

  def parse(uri : String, options : Mongo::Options) : Tuple(Array(Seed), Mongo::Options, Mongo::Credentials)
    scheme, scheme_rest = uri.split("://")

    raise Mongo::Error.new "Invalid scheme" unless scheme == "mongodb" || scheme == "mongodb+srv"

    path_split = scheme_rest.split('/', limit: 2)

    seeds = path_split[0].split(",")
    rest = path_split[1]?

    raise Mongo::Error.new "Invalid host" if seeds.any? &.empty?

    parsed_uri = ::URI.parse("#{scheme}://#{seeds[0]}/#{rest}")

    raise Mongo::Error.new "Trailing slash is required with options." if !parsed_uri.query.nil? && rest && rest.empty?

    database = parsed_uri.path
    database_has_forbidden_chars = false
    database.each_char { |char|
      next if database_has_forbidden_chars
      database_has_forbidden_chars = char == FORBIDDEN_DATABASE_CHARACTERS
    }

    raise Mongo::Error.new "Invalid database" if database_has_forbidden_chars

    # Validate by parsing every host
    seeds = seeds.map { |seed|
      uri = ::URI.parse("#{scheme}://#{seed}/#{rest}")
      port = uri.port.try &.to_i32 || 27017

      raise Mongo::Error.new "Invalid port" if port < 1 || port > 65535

      Seed.new(
        host: uri.hostname.try &.downcase || "localhost",
        port: uri.port.try &.to_i32 || 27017
      )
    }

    options.mix_with_query_params(parsed_uri.query_params)
    source = ::URI.decode(database[1..])
    if source.empty?
      source = options.auth_source
    end
    credentials = Mongo::Credentials.new(
      username: parsed_uri.user,
      password: parsed_uri.password,
      source: source || "",
      mechanism: options.auth_mechanism,
      mechanism_properties: options.auth_mechanism_properties
    )

    raise Mongo::Error.new "directConnection=true cannot be provided with multiple seeds" if options.direct_connection && seeds.size > 1

    {seeds, options, credentials}
  rescue e
    raise Mongo::Error.new "Invalid uri: #{uri}, #{e}"
  end
end
