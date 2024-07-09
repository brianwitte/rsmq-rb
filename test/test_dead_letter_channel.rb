require "minitest/autorun"
require "redis"
require "securerandom"
require "pry-byebug"

require_relative "../lib/rsmq-rb/rsmq"

class TestDeadLetterChannel < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name = "main_queue"
    @dead_letter_queue = "dead_letter_queue"
    @rsmq.create_queue(qname: @queue_name)
    @rsmq.create_queue(qname: @dead_letter_queue)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @rsmq.delete_queue(qname: @dead_letter_queue)
    @redis.flushdb
    @redis.close
  end

  def test_handling_undeliverable_messages
    sent_message = "Invalid format message"
    message_id = @rsmq.send_message(qname: @queue_name, message: sent_message)
    # Simulate a failure in processing
    move_to_dead_letter(@queue_name, @dead_letter_queue, message_id)

    # Verify the message is in the dead letter queue
    received_message = @rsmq.receive_message(qname: @dead_letter_queue)
    assert_equal(sent_message, received_message[:message])
  end

  def move_to_dead_letter(source_queue, target_queue, message_id)
    received_msg = @rsmq.receive_message(qname: source_queue)
    if received_msg[:id] && message_id == received_msg[:id]
      @rsmq.send_message(qname: target_queue, message: received_msg[:message])
      @rsmq.delete_message(qname: source_queue, id: message_id)
    end
  end
end
