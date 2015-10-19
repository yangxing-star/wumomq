
require 'test/unit'
require 'wumomq'

class IMConsumer < Wumomq::Consumer
  def process delivery_info, properties, payload
    puts "received : #{payload}"
    return true, false
  end
end

class IMConsumer2 < Wumomq::Consumer
  def initialize key
    @key = key
  end

  def process delivery_info, properties, payload
    puts "#{@key} received : #{payload}\n"
    return true, false
  end
end

class WumomqIMTest < Test::Unit::TestCase
  def test_im_base
    puts ' : IM queue test'
    puts 'start'
    mq = Wumomq::InstantMessage.new
    mq.subscribe(:key => 0, :consumer => IMConsumer.new)
    sleep 1
    mq.publish(:key => "1", :message => "hello world")
    sleep 10
    mq.cancel
    puts 'end'
  end

  def test_multi
    puts ' : IM multi queue test'
    puts 'start'
    mq = Wumomq::InstantMessage.new(:queue_num => 4)
    (0..(mq.get_number_of_subscribe - 1)).each { |i|
       Wumomq::InstantMessage.new(:queue_num => mq.get_number_of_subscribe).subscribe(:key => i, :consumer => IMConsumer2.new(i))
    }

    sleep 1
    (1..100).each { |i|
      mq.publish(:key => "#{i}", :message => "hello world #{i}")
    }
    sleep 10
    mq.cancel
    puts 'end'
  end
end