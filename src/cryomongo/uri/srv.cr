require "durian"

class Mongo::SRV
  def initialize(@resolver : Durian::Resolver, @url : String)
  end

  def resolve
    hostname, domainname = @url.split(".", 2)
    if !domainname.includes?('.')
      raise Mongo::Error.new("Top Level Domain is missing: #{domainname}")
    end

    srv_records = [] of Durian::Record::SRV
    txt_record : Durian::Record::TXT? = nil

    @resolver.resolve "_mongodb._tcp.#{hostname}.#{domainname}", [Durian::RecordFlag::SRV] do |response|
      response[0]?.try &.[2]?.try &.[0]?.try { |packet|
        srv_records = packet.answers.map &.resourceRecord.as(Durian::Record::SRV)
        packet.answers.each { |answer|
          srv_record = answer.resourceRecord.as(Durian::Record::SRV)
          if srv_record.target.split(".", 2)[1] != domainname
            raise Mongo::Error.new("SRV record has an invalid domain name: #{srv_record.target}")
          end
          srv_records << srv_record
        }
      }
    end

    @resolver.resolve "#{hostname}.#{domainname}", [Durian::RecordFlag::TXT] do |response|
      response[0]?.try &.[2]?.try &.[0]?.try { |packet|
        if packet.answers.size > 1
          raise Mongo::Error.new("#{packet.answers.size} TXT records were found when querying the DNS, but a single record is supported.")
        else
          txt_record = packet.answers.[0]?.try &.resourceRecord.as(Durian::Record::TXT)
        end
      }
    end

    @resolver.run

    if srv_records.empty?
      raise Mongo::Error.new("No SRV records found when querying url: #{@url}")
    end

    {srv_records, txt_record}
  end
end
