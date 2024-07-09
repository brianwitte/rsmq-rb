require 'minitest/autorun'
require 'redis'
require 'securerandom'
require 'pry-byebug'
require_relative '../lib/rsmq-rb/rsmq'

class TestPublishSubscribeChannel < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name1 = 'pub_sub1'
    @queue_name2 = 'pub_sub2'
    @rsmq.create_queue(qname: @queue_name1)
    @rsmq.create_queue(qname: @queue_name2)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name1)
    @rsmq.delete_queue(qname: @queue_name2)
    @redis.flushdb
    @redis.close
  end

  def test_message_broadcast_to_multiple_subscribers
    sent_message = "Hello, Publish Subscribe!"
    @rsmq.send_message(qname: @queue_name1, message: sent_message)
    @rsmq.send_message(qname: @queue_name2, message: sent_message)
    received_message1 = @rsmq.receive_message(qname: @queue_name1)
    received_message2 = @rsmq.receive_message(qname: @queue_name2)
    assert_equal(sent_message, received_message1[:message])
    assert_equal(sent_message, received_message2[:message])
  end
end
