require "uri"
require "./srv"
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
    '$',
  }

  def parse(uri : String, options : Mongo::Options) : Tuple(Array(Seed), Mongo::Options, Mongo::Credentials, String)
    scheme, scheme_rest = uri.split("://")

    raise Mongo::Error.new "Invalid scheme" unless scheme == "mongodb" || scheme == "mongodb+srv"

    path_split = scheme_rest.split('/', limit: 2)

    seeds = path_split[0].split(",")
    rest = path_split[1]?

    raise Mongo::Error.new "Invalid host" if seeds.any? &.empty?

    parsed_uri = ::URI.parse("#{scheme}://#{seeds[0]}/#{rest}")

    raise Mongo::Error.new "Trailing slash is required with options." if !parsed_uri.query.nil? && rest && rest.empty?

    query_params = parsed_uri.query_params

    if scheme == "mongodb+srv"
      if seeds.size > 1
        raise Mongo::Error.new "Cannot specify more than one host name in a connection string with the mongodb+srv protocol."
      end
      if parsed_uri.port
        raise Mongo::Error.new "Cannot specify a port in a connection string with the mongodb+srv protocol."
      end
      srv = Mongo::SRV.new(options.dns_resolver, parsed_uri.host.not_nil!)
      srv_records, txt_record = srv.resolve
      seeds = srv_records.map { |srv_record|
        "#{srv_record.target}:#{srv_record.port}"
      }

      query_params["ssl"] = "true" unless query_params.has_key? "ssl"
      txt_record.try { |txt|
        txt_options = ::URI::Params.parse(txt.txt)
        {"authSource", "replicaSet"}.each { |key|
          if txt_options.has_key?(key) && !query_params.has_key?(key)
            query_params[key] = txt_options[key]
          end
        }
        txt_options.each { |option, _|
          case option
          when "authSource"
          when "replicaSet"
            # ok
          else
            raise Mongo::Error.new("Invalid TXT record option: #{option}")
          end
        }
      }
    end

    default_auth_db = ::URI.decode(parsed_uri.path[1..])
    has_forbidden_chars = false
    default_auth_db.each_char { |char|
      break if has_forbidden_chars
      has_forbidden_chars = char.in?(FORBIDDEN_DATABASE_CHARACTERS)
    }

    raise Mongo::Error.new "Invalid database" if has_forbidden_chars

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

    options.mix_with_query_params(query_params)
    source = options.auth_source || default_auth_db
    credentials = Mongo::Credentials.new(
      username: parsed_uri.user,
      password: parsed_uri.password,
      source: source || "",
      mechanism: options.auth_mechanism,
      mechanism_properties: options.auth_mechanism_properties
    )

    raise Mongo::Error.new "directConnection=true cannot be provided with multiple seeds" if options.direct_connection && seeds.size > 1

    {seeds, options, credentials, default_auth_db}
  rescue e
    raise Mongo::Error.new "Invalid uri: #{uri}, #{e}"
  end
end
