require "minitest/autorun"
require "redis"
require "securerandom"
require "pry-byebug"

require_relative "../lib/rsmq-rb/rsmq"

class TestStreamAggregator < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "stream_aggregator")
    @queue_name = "stream_queue"
    # Ensuring a longer visibility timeout
    @rsmq.create_queue(qname: @queue_name, vt: 30)

    5.times do |i|
      sequence_number = i + 1
      data_packet = (i + 1) * 100
      message = "#{sequence_number.to_s.rjust(2, "0")}: Data Packet #{data_packet}"
      @rsmq.send_message(qname: @queue_name, message: message)
    end
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @redis.flushdb
    @redis.close
  end

  def test_ordered_stream_aggregation
    expected_result = "01: Data Packet 100, 02: Data Packet 200, 03: Data Packet 300, 04: Data Packet 400, 05: Data Packet 500"
    message_buffer = []
    attempts = 0
    # Increased maximum attempts
    max_attempts = 50

    while message_buffer.size < 5 && attempts < max_attempts
      msg = @rsmq.receive_message(qname: @queue_name)
      if msg[:message].nil?
        # puts("Attempt #{attempts + 1}: No message received, retrying...")
        # Pause briefly b/t retries to avoid overwhelming the Redis
        # server and to simulate "realistic" message polling behavior.
        sleep(1)
        attempts += 1
        next
      end

      seq_num, data = msg[:message].split(": ", 2)
      seq_num = seq_num.to_i

      # Add the new message to the buffer, encapsulating the sequence number and its associated data.
      message_buffer << {seq: seq_num, data: data}

      # Ensure each message in the buffer is unique by sequence number to avoid processing duplicates.
      message_buffer.uniq! do |m|
        # Extract sequence number for uniqueness check.
        m[:seq]
      end

      # Sort the messages in the buffer by sequence number to prepare them for ordered processing.
      message_buffer.sort_by! do |m|
        # Use the sequence number as the sorting criterion.
        m[:seq]
      end

      # Output the details of the received message for debugging and verification of stream processing.
      # puts("Received: #{msg[:message]} - Buffer now contains #{message_buffer.size} messages.")
    end

    if attempts >= max_attempts
      raise "Test failed: Timeout reached before all messages were processed."
    end

    # NOTE: `sorted_messages` holds the formatted output.  Actual
    #        sorting was done earlier in the buffer.
    sorted_messages = message_buffer.map { |m| "#{m[:seq].to_s.rjust(2, "0")}: #{m[:data]}" }.join(", ")
    assert_equal(expected_result, sorted_messages)
  end
end
