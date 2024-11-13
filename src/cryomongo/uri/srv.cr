require "dns"
require "dns/resource/srv"

class Mongo::SRV
  def initialize(@url : String)
  end

  def resolve
    hostname, domainname = @url.split(".", 2)
    if !domainname.includes?('.')
      raise Mongo::Error.new("Top Level Domain is missing: #{domainname}")
    end

    srv_records = [] of DNS::Resource::SRV
    txt_record : DNS::Resource::TXT? = nil

    DNS.query "_mongodb._tcp.#{hostname}.#{domainname}", [DNS::RecordType::SRV] do |answer|
      srv_record = answer.resource.as(DNS::Resource::SRV)
      if srv_record.target.split(".", 2)[1] != domainname
        raise Mongo::Error.new("SRV record has an invalid domain name: #{srv_record.target}")
      end
      srv_records << srv_record
    end

    DNS.query "#{hostname}.#{domainname}", [DNS::RecordType::TXT] do |answer|
      txt_record = answer.resource.as(DNS::Resource::TXT)

      number_of_txt_records = txt_record.text_data.size
      if number_of_txt_records != 1
        raise Mongo::Error.new("#{number_of_txt_records} TXT records were found when querying the DNS, but a single record is supported.")
      end
    end

    if srv_records.empty?
      raise Mongo::Error.new("No SRV records found when querying url: #{@url}")
    end

    {srv_records, txt_record}
  end
end
