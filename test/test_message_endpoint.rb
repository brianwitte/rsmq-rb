require 'minitest/autorun'
require 'redis'
require 'securerandom'
require 'pry-byebug'
require_relative '../lib/rsmq-rb/rsmq'

class TestMessageEndpoint < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name = 'message_endpoint'
    @rsmq.create_queue(qname: @queue_name)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @redis.flushdb
    @redis.close
  end

  def test_send_and_receive_messages
    sent_message = "Endpoint test message"
    @rsmq.send_message(qname: @queue_name, message: sent_message)
    received_message = @rsmq.receive_message(qname: @queue_name)

    assert_equal(sent_message, received_message[:message])
  end
end
