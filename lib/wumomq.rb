require 'wumomq/version'
require 'bunny'

module Wumomq
  class Consumer
    def process delivery_info, properties, payload
      raise NotImplementedError, "Implement this method in a child class"

      # 应该返回2个值，succeed，abort
    end
  end

  class Base
    def initialize uri, queue_name, queue_num = 1, exchange_name = nil
      @conn      = Bunny.new uri
      @conn.start
      @channel   = @conn.create_channel
      @num       = queue_num
      @queue     = Array.new
      (0..(@num - 1)).each { |i|
        @queue.push(@channel.queue("#{queue_name}.#{i}", :auto_delete => false))
      }

      @consumer  = nil
      @exchange  = @channel.default_exchange
    end

    def get_number_of_subscribe
      @num
    end

    def hash value
      Digest::MD5.hexdigest(value)[0...4].to_i(16) % @num
    end

    def publish options = {}
      (puts "没有消息主体"; return nil) if options[:message].nil? 
      (puts "没有指定key"; return nil) if options[:key].nil?
      q = hash(options[:key])
      @exchange.publish(options[:message], :routing_key => @queue[q].name)
    end

    def subscribe options = {}
      # 在登记的时候, key应该是整数
      (puts "没有处理的consumer"; return false) if options[:consumer].nil? 
      (puts "key为空"; return false) if options[:key].nil?

      @consumer = @queue[options[:key]].subscribe(:manual_ack => true, :block => false) do |delivery_info, properties, payload|
         succeed, abort = options[:consumer].process delivery_info, properties, payload
         @channel.acknowledge delivery_info.delivery_tag, false if succeed || abort
      end

      true
    end

    def cancel
      @consumer.cancel if @consumer
    end

    def close
      @conn.close
    end
  end

  class User < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_user_uri"]
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = "wumo.user.inbox"
      super uri, queue_name, queue_num
    end
  end

  class HR < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_hr_uri"]
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = "wumo.hr.inbox"
      super uri, queue_name, queue_num
    end
  end

  class OA < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_oa_uri"]
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = "wumo.oa.inbox"
      super uri, queue_name, queue_num
    end
  end

  class Calendar < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_oa_uri"]
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = "wumo.oa.inbox"
      super uri, queue_name, queue_num
    end
  end

  class Backyard < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_backyard_uri"]
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = "wumo.backyard.inbox"
      super uri, queue_name, queue_num
    end
  end

  class InstantMessage < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_im_uri"]
      type       = options[:type] || type = ENV["wumomq_im_type"] || type = "message"
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = (type == "message" ? "wumo.im.inbox" : "wumo.im.sync")
      super uri, queue_name, queue_num
    end
  end

  class Sms < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_sms_uri"]
      queue_num  = options[:queue_num] || queue_num = 1
      queue_name = "wumo.sms.inbox"
      super uri, queue_name, queue_num
    end
  end
end
