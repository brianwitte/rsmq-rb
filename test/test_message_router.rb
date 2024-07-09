require 'minitest/autorun'
require 'redis'
require 'securerandom'
require 'pry-byebug'
require_relative '../lib/rsmq-rb/rsmq'

class TestMessageRouter < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name = 'router'
    @rsmq.create_queue(qname: @queue_name)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @redis.flushdb
    @redis.close
  end

  def test_route_based_on_content
    sent_message = "type:admin;action:report"
    @rsmq.send_message(qname: @queue_name, message: sent_message)
    received_message = @rsmq.receive_message(qname: @queue_name)
    # Simulate routing logic
    route = received_message[:message].split(';').first.split(':').last == 'admin' ? 'admin_queue' : 'user_queue'
    assert_equal('admin_queue', route)
  end
end
