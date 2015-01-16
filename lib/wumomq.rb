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
    def initialize uri, queue_name, exchange_name = nil
      @conn      = Bunny.new uri
      @conn.start
      @channel   = @conn.create_channel
      @queue     = @channel.queue(queue_name, :auto_delete => false)
      @consumer  = nil
      @exchange  = @channel.default_exchange
    end

    def publish options = {}
      return nil if options[:message].nil?
      @exchange.publish(options[:message], :routing_key => @queue.name)
    end

    def subscribe options = {}
      return false if options[:consumer].nil?

      @consumer = @queue.subscribe(:manual_ack => true, :block => false) do |delivery_info, properties, payload|
         succeed, abort = options[:consumer].process delivery_info, properties, payload
         @channel.acknowledge delivery_info.delivery_tag, false if succeed || abort
      end

      true
    end

    def cancel
      @consumer.cancel if @consumer
    end
  end

  class User < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_user_uri"]
      queue_name = "wumo.user.inbox"
      super uri, queue_name
    end
  end

  class HR < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_hr_uri"]
      queue_name = "wumo.hr.inbox"
      super uri, queue_name
    end
  end

  class OA < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_oa_uri"]
      queue_name = "wumo.oa.inbox"
      super uri, queue_name
    end
  end

  class Calendar < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_oa_uri"]
      queue_name = "wumo.oa.inbox"
      super uri, queue_name
    end
  end

  class Backyard < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_backyard_uri"]
      queue_name = "wumo.backyard.inbox"
      super uri, queue_name
    end
  end

  class InstantMessage < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_im_uri"]
      type       = options[:type] || type = ENV["wumomq_im_type"] || type = "message"
      queue_name = (type == "message" ? "wumo.im.inbox" : "wumo.im.sync")
      super uri, queue_name
    end
  end

  class Sms < Base
    def initialize options = {}
      uri        = options[:uri]  || uri  = ENV["wumomq_sms_uri"]
      queue_name = "wumo.sms.inbox"
      super uri, queue_name
    end
  end
end
