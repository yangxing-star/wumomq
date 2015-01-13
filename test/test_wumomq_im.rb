
require 'test/unit'
require 'wumomq'

class IMConsumer < Wumomq::Consumer
  def process delivery_info, properties, payload
    puts "received : #{payload}"
    return true, false
  end
end

class WumomqIMTest < Test::Unit::TestCase
  def test_im_base
      puts ' : IM queue test'
      puts 'start'
      mq = Wumomq::InstantMessage.new
      mq.subscribe(:consumer => IMConsumer.new)
      sleep 1
      mq.publish(:message => "hello world")
      sleep 10
      mq.cancel
      puts 'end'
  end
end

