# A set of options used to configure the driver.
#
# NOTE: [For more details, see the uri options specification document](https://github.com/mongodb/specifications/blob/master/source/uri-options/uri-options.rst).
struct Mongo::Options
  include Tools::Initializer

  # Passed into the server in the client metadata as part of the connection handshake
  getter appname : String? = nil
  # The authentication mechanism method to use for connection to the server
  getter auth_mechanism : String? = nil
  # Additional options provided for authentication (e.g. to enable hostname canonicalization for GSSAPI)
  getter auth_mechanism_properties : String? = nil
  # The database that connections should authenticate against
  getter auth_source : String? = nil
  # The list of allowed compression types for wire protocol messages sent or received from the server
  getter compressors : String? = nil
  # Amount of time to wait for a single TCP socket connection to the server to be established before erroring; note that this applies to SDAM isMaster operations
  getter connect_timeout : Time::Span? = nil # 10.seconds
  # Whether to connect to the deployment in Single topology.
  getter direct_connection : Bool? = nil
  # The interval between regular server monitoring checks
  property heartbeat_frequency : Time::Span = 10.seconds
  # Default write concern "j" field for the client
  getter journal : Bool? = nil
  # The amount of time beyond the fastest round trip time that a given server’s round trip time can take and still be eligible for server selection
  getter local_threshold : Time::Span = 15.milliseconds
  # The amount of time a connection can be idle before it's closed
  getter max_idle_time : Time::Span? = nil
  # The maximum number of clients or connections able to be created by a pool at a given time
  getter max_pool_size : Int32 = 100
  # The maximum replication lag, in wall clock time, that a secondary can suffer and still be eligible for server selection
  getter max_staleness_seconds : Int32? = nil
  # The maximum number of clients or connections able to be created by a pool at a given time
  getter min_pool_size : Int32 = 1
  # Default read concern for the client
  getter read_concern_level : String? = nil
  # Default read preference for the client (excluding tags)
  getter read_preference : String? = nil
  # Default read preference tags for the client; only valid if the read preference mode is not primary
  getter read_preference_tags : Array(String) = [] of String
  # The name of the replica set to connect to
  getter replica_set : String? = nil
  # Enables retryable reads on server 3.6+
  getter retry_reads : Bool? = nil
  # Enables retryable writes on server 3.6+
  getter retry_writes : Bool? = nil
  # A timeout in milliseconds to block for server selection before raising an error
  property server_selection_timeout : Time::Span = 30.seconds
  # Scan the topology only once after a server selection failure instead of repeatedly until the server selection times out
  property server_selection_try_once : Bool = true
  # Amount of time spent attempting to send or receive on a socket before timing out; note that this only applies to application operations, not SDAM
  getter socket_timeout : Time::Span? = nil
  # Alias of "tls"; required to ensure that Atlas connection strings continue to work
  getter ssl : Bool? = nil
  # Whether or not to require TLS for connections to the server
  getter tls : Bool? = nil
  # Specifies whether or not the driver should error when the server’s TLS certificate is invalid
  getter tls_allow_invalid_certificates : Bool? = nil
  # Specifies whether or not the driver should error when there is a mismatch between the server’s hostname and the hostname specified by the TLS certificate
  getter tls_allow_invalid_hostnames : Bool? = nil
  # Path to file with either a single or bundle of certificate authorities to be considered trusted when making a TLS connection
  getter tls_ca_file : String? = nil
  # Path to the client certificate file or the client private key file; in the case that they both are needed, the files should be concatenated
  getter tls_certificate_key_file : String? = nil
  # Password to decrypt the client private key to be used for TLS connections
  getter tls_certificate_key_file_password : String? = nil
  # Controls whether or not the driver will check a certificate's revocation status via CRLs or OCSP. See the OCSP Support Spec for additional information.
  getter tls_disable_certificate_revocation_check : Bool? = nil
  # Controls whether or not the driver will reach out to OCSP endpoints if needed. See the OCSP Support Spec for additional information.
  getter tls_disable_ocsp_endpoint_check : Bool? = nil
  # Relax TLS constraints as much as possible (e.g. allowing invalid certificates or hostname mismatches); drivers must document the exact constraints which are relaxed by this option being true
  getter tls_insecure : Bool? = nil
  # Default write concern "w" field for the client
  getter w : Int32? = nil
  # The maximum amount of time a fiber can wait for a connection to become available
  getter wait_queue_timeout : Time::Span? = nil
  # Default write concern "wtimeout" field for the client
  getter w_timeout : Time::Span? = nil
  # Specifies the level of compression when using zlib to compress wire protocol messages; -1 signifies the default level, 0 signifies no compression, 1 signifies the fastest speed, and 9 signifies the best compression
  getter zlib_compression_level : Int32? = nil

  getter! raw : HTTP::Params

  def mix_with_query_params(options_hash : HTTP::Params)
    @raw = HTTP::Params.parse HTTP::Params.build { |form|
      options_hash.each { |key, value|
        form.add key.downcase, value
      }
    }

    validate(raw)

    {% begin %}
      {% for ivar in @type.instance_vars %}
        {% default_value = ivar.default_value %}
        {% types = ivar.type.union_types %}

        {% if types.includes? Time::Span %}
          {% option_name = ivar.name.gsub(/_/, "").stringify + "ms" %}
        {% else %}
          {% option_name = ivar.name.gsub(/_/, "").stringify %}
        {% end %}
        option = raw[{{option_name}}]?

        unless option.nil? || option.empty?
          unless @{{ivar.name.id}} != {{default_value}}
            begin
              {% if types.includes? Bool %}
                if option == "true"
                  @{{ivar.name.id}} = true
                elsif option == "false"
                  @{{ivar.name.id}} = false
                end
              {% elsif types.includes? Int32 %}
                @{{ivar.name.id}} = option.to_i32
              {% elsif types.includes? Int64 %}
                @{{ivar.name.id}} = option.to_i64
              {% elsif types.includes? Time::Span %}
                @{{ivar.name.id}} = option.to_i32.milliseconds
              {% elsif types.includes? String %}
                @{{ivar.name.id}} = option
              {% elsif types.includes? Array %}
                @{{ivar.name.id}} = raw.fetch_all({{option_name}})
              {% end %}
            rescue e
              ::Mongo::Log.warn { %(option "#{{{option_name}}}" has invalid value: "#{option}".) }
            end
          end
        end
      {% end %}
    {% end %}
  end

  def validate(raw_hash)
    if raw_hash.has_key?("tlsinsecure") && raw_hash.has_key?("tlsallowinvalidcertificates")
      raise Mongo::Error.new "tlsInsecure and tlsAllowInvalidCertificates cannot be both present"
    end
    if raw_hash.has_key?("tlsinsecure") && raw_hash.has_key?("tlsallowinvalidhostnames")
      raise Mongo::Error.new "tlsInsecure and tlsAllowInvalidHostnames cannot be both present"
    end
    if raw_hash.has_key?("tlsdisablecertificaterevocationcheck") && raw_hash.has_key?("tlsallowinvalidcertificates")
      raise Mongo::Error.new "tlsDisableCertificateRevocationCheck and tlsAllowInvalidCertificates cannot be both present"
    end
    if raw_hash.has_key?("tlsinsecure") && raw_hash.has_key?("tlsdisablecertificaterevocationcheck")
      raise Mongo::Error.new "tlsInsecure and tlsDisableCertificateRevocationCheck cannot be both present"
    end
    if raw_hash.has_key?("tlsdisablecertificaterevocationcheck") && raw_hash.has_key?("tlsdisableocspendpointcheck")
      raise Mongo::Error.new "tlsDisableCertificateRevocationCheck and tlsDisableOCSPEndpointCheck cannot be both present"
    end
    if raw_hash.has_key?("tlsinsecure") && raw_hash.has_key?("tlsdisableocspendpointcheck")
      raise Mongo::Error.new "tlsInsecure and tlsDisableOCSPEndpointCheck cannot be both present"
    end
    if raw_hash.has_key?("tlsallowinvalidcertificates") && raw_hash.has_key?("tlsdisableocspendpointcheck")
      raise Mongo::Error.new "tlsAllowInvalidCertificates and tlsDisableOCSPEndpointCheck cannot be both present"
    end
    if raw_hash.has_key?("tls") && raw_hash.has_key?("ssl") && raw_hash["ssl"] != raw_hash["tls"]
      raise Mongo::Error.new "tls and ssl have different values"
    end
  end
end
