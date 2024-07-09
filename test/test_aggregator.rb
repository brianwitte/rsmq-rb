require "minitest/autorun"
require "redis"
require "securerandom"
require "pry-byebug"

require_relative "../lib/rsmq-rb/rsmq"

class TestAggregator < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "localhost", port: 6379, ns: "test_aggregator")
    @unordered_queue_name = "unordered_log_queue"
    @rsmq.create_queue(qname: @unordered_queue_name)
    5.times { |i| @rsmq.send_message(qname: @unordered_queue_name, message: "Log entry #{rand(1000)}") }
  end

  def teardown
    @rsmq.delete_queue(qname: @unordered_queue_name)
    @redis.flushdb
  end

  # Test unordered aggregation of messages.
  # This scenario is suitable for use cases where the order does not matter,
  # such as aggregating logs for statistical analysis or monitoring.
  def test_unordered_aggregation
    logs = []

    # Retrieve messages as they arrive, without concern for order.
    5.times do
      msg = @rsmq.receive_message(qname: @unordered_queue_name)
      logs << msg[:message] if msg
    end

    # Verify that all log messages are received, regardless of order.
    assert_equal(5, logs.size)
  end
end
