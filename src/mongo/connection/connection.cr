require "./credentials"

struct Mongo::Connection
  getter server_description : SDAM::ServerDescription
  getter credentials : Mongo::Credentials
  getter socket : IO

  def initialize(@server_description : SDAM::ServerDescription, @credentials : Mongo::Credentials, @options : Mongo::Options)
    if @server_description.address.ends_with? ".sock"
      socket = UNIXSocket.new(@server_description.address)
    else
      split = @server_description.address.split(':')
      socket = TCPSocket.new(split[0], split[1]? || 27017)
    end

    if @options.ssl || @options.tls
      context = OpenSSL::SSL::Context::Client.new
      if tls_ca_file = @options.tls_ca_file
        context.ca_certificates = tls_ca_file
      end
      if @options.tls_insecure || @options.tls_allow_invalid_certificates
        context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
      end
      context.add_options(OpenSSL::SSL::Options::ALL)
      context.add_options(OpenSSL::SSL::Options.flags(
        NO_SSL_V2,
        NO_COMPRESSION,
        NO_SESSION_RESUMPTION_ON_RENEGOTIATION
      ))
      socket = OpenSSL::SSL::Socket::Client.new(socket, context, sync_close: true)
    end

    @socket = socket
  end

  def before_checkout
  end

  def close
    @socket.close unless @socket.closed?
  end
end
