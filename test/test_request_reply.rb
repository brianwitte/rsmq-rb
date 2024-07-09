require 'minitest/autorun'
require 'redis'
require 'securerandom'
require 'pry-byebug'
require_relative '../lib/rsmq-rb/rsmq'

class TestRequestReply < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @request_queue = 'request_queue'
    @reply_queue = 'reply_queue'
    @rsmq.create_queue(qname: @request_queue)
    @rsmq.create_queue(qname: @reply_queue)
  end

  def teardown
    @rsmq.delete_queue(qname: @request_queue)
    @rsmq.delete_queue(qname: @reply_queue)
    @redis.flushdb
    @redis.close
  end

  def test_request_reply_pattern
    request_message = "Request data"
    reply_message = "Reply with data"
    
    @rsmq.send_message(qname: @request_queue, message: request_message)
    process_request_and_send_reply(@request_queue, @reply_queue, reply_message)
    
    received_reply = @rsmq.receive_message(qname: @reply_queue)
    assert_equal(reply_message, received_reply[:message])
  end

  def process_request_and_send_reply(request_queue, reply_queue, reply_message)
    request = @rsmq.receive_message(qname: request_queue)
    @rsmq.send_message(qname: reply_queue, message: reply_message) if request
  end
end
