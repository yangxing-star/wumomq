require 'wumomq/version'
require 'bunny'

module Wumomq
  class Consumer
    def process delivery_info, properties, payload
      raise NotImplementedError, "Implement this method in a child class"

      # return 2 params，succeed，abort
    end
  end

  class Base
    def initialize uri, queue_name, queue_num = 1, exchange_name = nil
      @conn      = Bunny.new uri
      @conn.start
      @channel   = @conn.create_channel
      @num       = queue_num
      @queue     = Array.new
      (0...@num).each { |i|
        @queue.push(@channel.queue("#{queue_name}.#{i}", auto_delete: false))
      }

      @consumer  = nil
      @exchange  = @channel.default_exchange
    end

    def get_number_of_subscribe
      @num
    end

    def get_queue
      @queue
    end

    def status
      @conn.status
    end

    def hash value
      Digest::MD5.hexdigest(value.to_s)[0...4].to_i(16) % @num
    end

    def publish options = {}
      (puts "没有消息主体(#{__FILE__}.#{__LINE__})"; return nil) if options[:message].nil? 
      (puts "没有指定key(#{__FILE__}.#{__LINE__})"; return nil) if options[:key].nil?
      q = hash(options[:key])
      @exchange.publish(options[:message], routing_key: @queue[q].name)

      sleep 1
      close
    end

    def subscribe options = {}
      begin
        # 在登记的时候, key应该是整数
        (puts "没有处理的consumer(#{__FILE__}.#{__LINE__})"; return false) if options[:consumer].nil? 
        (puts "key为空(#{__FILE__}.#{__LINE__})"; return false) if options[:key].nil?

        @consumer = @queue[options[:key]].subscribe(manual_ack: true, block: false) do |delivery_info, properties, payload|
          succeed, abort = options[:consumer].process delivery_info, properties, payload
          @channel.acknowledge delivery_info.delivery_tag, false if succeed || abort
        end
        true
      rescue Interrupt => _
        close
        false
      end
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
      uri        = options[:uri] || ENV['user_uri']
      queue_num  = options[:queue_num] || ENV['user_queue_nb'] || 1
      queue_name = 'user.inbox'
      super uri, queue_name, queue_num
    end
  end

  class HR < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['hr_uri']
      queue_num  = options[:queue_num] || ENV['hr_queue_nb'] || 1
      queue_name = 'hr.inbox'
      super uri, queue_name, queue_num
    end
  end

  class OA < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['oa_uri']
      queue_num  = options[:queue_num] || ENV['oa_queue_nb'] || 1
      queue_name = 'oa.inbox'
      super uri, queue_name, queue_num
    end
  end

  class Calendar < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['cal_uri']
      queue_num  = options[:queue_num] || ENV['cal_queue_nb'] || 1
      queue_name = 'oa.inbox'
      super uri, queue_name, queue_num
    end
  end

  class Backyard < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['backyard_uri']
      queue_num  = options[:queue_num] || ENV['backyard_queue_nb'] || 1
      queue_name = 'backyard.inbox'
      super uri, queue_name, queue_num
    end
  end

  class InstantMessage < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['im_uri']
      queue_num  = options[:queue_num] || ENV['im_queue_nb'] || 1
      queue_name = 'im.inbox'
      super uri, queue_name, queue_num
    end
  end

  class IMSync < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['imsync_uri']
      queue_num  = options[:queue_num] || ENV['imsync_queue_nb'] || 1
      queue_name = 'im.sync'
      super uri, queue_name, queue_num
    end
  end

  class Sms < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['sms_uri']
      queue_num  = options[:queue_num] || ENV['sms_queue_nb'] || 1
      queue_name = 'sms.inbox'
      super uri, queue_name, queue_num
    end
  end
  class Delay < Base
    def initialize options = {}
      uri        = options[:uri] || ENV['delay_uri']
      queue_num  = options[:queue_num] || ENV['delay_queue_nb'] || 1
      queue_name = 'delay.inbox'
      super uri, queue_name, queue_num
    end
  end
end