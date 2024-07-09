require "minitest/autorun"
require "redis"
require "securerandom"
require "pry-byebug"

require_relative '../lib/rsmq-rb/rsmq'


class TestRsmq < Minitest::Test
  def setup
    @redis = Redis.new
    # This clears all keys in the current database
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @rsmq2 = Rsmq.new(client: @redis)
    @q1_length = 0
    @queue1 = {name: "test1"}
    @queue2 = {name: "test2"}
    @queue3 = {name: "test3promises", m1: "Hello", m2: "World"}
    @q1m1 = nil
    @q1m2 = nil
    @q1m3 = nil
    @q2m2 = nil
    @q2msgs = {}
  end

  def teardown
    # Don't always clear queues to allow checking their state in tests
    # @redis.flushdb # Uncomment this line if you want to clear the entire Redis database after each test
  end

  def receive_all_messages(qname, vt: 2, max_messages: nil)
    received_msgs = []
    loop do
      msg = @rsmq.receive_message(qname: qname, vt: vt)
      break if msg[:id].nil? || msg.empty?
      received_msgs << msg
      break if max_messages && received_msgs.size >= max_messages
    end

    received_msgs
  end

  def looong_string
    "HELLOWORLD!" * 6500
  end

  def test_get_redis_smq_instance
    assert_instance_of(Rsmq, @rsmq)
  end

  def test_use_existing_redis_client
    assert_instance_of(Rsmq, @rsmq2)
  end

  def test_delete_all_leftover_queues
    @rsmq.delete_queue(qname: @queue1[:name]) rescue nil
    @rsmq.delete_queue(qname: @queue2[:name]) rescue nil
    @rsmq.delete_queue(qname: @queue3[:name]) rescue nil
    sleep(0.1)
  end

  def test_create_queue
    assert_equal(1, @rsmq.create_queue(qname: @queue3[:name], vt: 0, maxsize: -1))
  end

  def test_send_message
    @rsmq.create_queue(qname: @queue3[:name])
    assert(
      @rsmq.send_message(qname: @queue3[:name], message: @queue3[:m1])
    )
  end

  def test_send_another_message
    @rsmq.create_queue(qname: @queue3[:name])
    assert(@rsmq.send_message(qname: @queue3[:name], message: @queue3[:m2]))
  end

  def test_check_maxsize_of_queue
    @rsmq.create_queue(qname: @queue3[:name])
    attrs = @rsmq.get_queue_attributes(qname: @queue3[:name])
    assert_equal(65_536, attrs["maxsize"].to_i)
  end

  def test_receive_message
    @rsmq.create_queue(qname: @queue3[:name])
    @rsmq.send_message(qname: @queue3[:name], message: @queue3[:m1])
    msg = @rsmq.receive_message(qname: @queue3[:name], vt: 2)
    assert_equal(@queue3[:m1], msg[:message])
  end

  def test_receive_another_message
    @rsmq.create_queue(qname: @queue3[:name])
    @rsmq.send_message(qname: @queue3[:name], message: @queue3[:m2])
    msg = @rsmq.receive_message(qname: @queue3[:name], vt: 1)
    assert_equal(@queue3[:m2], msg[:message])
  end

  def test_fail_receive_another_message_no_available_message
    @rsmq.create_queue(qname: @queue3[:name])
    msg = @rsmq.receive_message(qname: @queue3[:name], vt: 1)
    assert_nil(msg[:id])
  end

  def test_wait
    sleep(1.01)
  end

  def test_receive_message_again
    @rsmq.create_queue(qname: @queue3[:name])
    @rsmq.send_message(qname: @queue3[:name], message: @queue3[:m2])
    msg = @rsmq.receive_message(qname: @queue3[:name], vt: 3)
    assert_equal(@queue3[:m2], msg[:message])
  end

  def test_wait_again
    sleep(1.01)
  end

  def test_receive_message_again_2
    @rsmq.create_queue(qname: @queue3[:name])
    @rsmq.send_message(qname: @queue3[:name], message: @queue3[:m1])
    msg = @rsmq.receive_message(qname: @queue3[:name], vt: 3)
    assert_equal(@queue3[:m1], msg[:message])
  end

  def test_delete_created_queue
    @rsmq.create_queue(qname: @queue3[:name])
    assert_equal(1, @rsmq.delete_queue(qname: @queue3[:name]))
  end

  def test_create_queue_invalid_name
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: "should throw") }
  end

  def test_create_queue_long_name
    long_name = "long_name" * 100
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: long_name) }
  end

  def test_create_queue_negative_vt
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], vt: -20) }
  end

  def test_create_queue_non_numeric_vt
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], vt: "not_a_number") }
  end

  def test_create_queue_high_vt
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], vt: 10_000_000) }
  end

  def test_create_queue_negative_delay
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], delay: -20) }
  end

  def test_create_queue_non_numeric_delay
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], delay: "not_a_number") }
  end

  def test_create_queue_high_delay
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], delay: 10_000_000) }
  end

  def test_create_queue_negative_maxsize
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], maxsize: -20) }
  end

  def test_create_queue_non_numeric_maxsize
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], maxsize: "not_a_number") }
  end

  def test_create_queue_high_maxsize
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], maxsize: 66_000) }
  end

  def test_create_queue_low_maxsize
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name], maxsize: 900) }
  end

  def test_list_queues_empty
    assert_equal([], @rsmq.list_queues)
  end

  def test_create_queue1
    assert_equal(1, @rsmq.create_queue(qname: @queue1[:name]))
  end

  def test_list_queues_one_element
    @rsmq.create_queue(qname: @queue1[:name])
    assert_equal([@queue1[:name]], @rsmq.list_queues)
  end

  def test_create_queue2
    assert_equal(1, @rsmq.create_queue(qname: @queue2[:name], maxsize: 1024))
  end

  def test_list_queues_two_elements
    @rsmq.create_queue(qname: @queue1[:name])
    @rsmq.create_queue(qname: @queue2[:name])
    assert_equal([@queue1[:name], @queue2[:name]].sort, @rsmq.list_queues.sort)
  end

  def test_get_queue_attributes_queue1
    @rsmq.create_queue(qname: @queue1[:name])
    attrs = @rsmq.get_queue_attributes(qname: @queue1[:name])
    assert_equal(0, attrs["msgs"].to_i)
    @queue1[:modified] = attrs["modified"]
  end

  def test_get_queue_attributes_bogus_queue
    assert_raises(Rsmq::Error) { @rsmq.get_queue_attributes(qname: "sdfsdfsdf") }
  end

  def test_set_queue_attributes_bogus_queue_no_attributes
    assert_raises(Rsmq::Error) { @rsmq.set_queue_attributes(qname: "kjdsfh3h") }
  end

  def test_set_queue_attributes_bogus_queue_with_attributes
    assert_raises(Rsmq::Error) { @rsmq.set_queue_attributes(qname: "kjdsfh3h", vt: 1000) }
  end

  def test_set_queue_attributes_vt
    @rsmq.create_queue(qname: @queue1[:name])
    attrs = @rsmq.set_queue_attributes(qname: @queue1[:name], vt: 1234)
    assert_equal(1234, attrs["vt"].to_i)
    assert_equal(0, attrs["delay"].to_i)
    assert_equal(65536, attrs["maxsize"].to_i)
  end

  def test_set_queue_attributes_delay
    @rsmq.create_queue(qname: @queue1[:name])
    sleep(1.1)
    attrs = @rsmq.set_queue_attributes(qname: @queue1[:name], delay: 7)
    assert_equal(7, attrs["delay"].to_i)
    assert(attrs["modified"].to_i > @queue1[:modified].to_i)
  end

  def test_set_queue_attributes_unlimited_maxsize
    @rsmq.create_queue(qname: @queue1[:name])
    attrs = @rsmq.set_queue_attributes(qname: @queue1[:name], maxsize: -1)
    assert_equal(-1, attrs["maxsize"].to_i)
  end

  def test_set_queue_attributes_maxsize
    @rsmq.create_queue(qname: @queue1[:name])
    attrs = @rsmq.set_queue_attributes(qname: @queue1[:name], maxsize: 1024)
    assert_equal(1024, attrs["maxsize"].to_i)
  end

  def test_set_queue_attributes_all
    @rsmq.create_queue(qname: @queue1[:name])
    attrs = @rsmq.set_queue_attributes(qname: @queue1[:name], maxsize: 65536, vt: 30, delay: 0)
    assert_equal(30, attrs["vt"].to_i)
    assert_equal(0, attrs["delay"].to_i)
    assert_equal(65536, attrs["maxsize"].to_i)
  end

  def test_set_queue_attributes_small_maxsize
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(Rsmq::Error) { @rsmq.set_queue_attributes(qname: @queue1[:name], maxsize: 50) }
  end

  def test_set_queue_attributes_negative_vt
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(Rsmq::Error) { @rsmq.set_queue_attributes(qname: @queue1[:name], vt: -5) }
  end

  def test_send_message_non_existing_queue
    assert_raises(Rsmq::Error) { @rsmq.send_message(qname: "rtlbrmpft", message: "foo") }
  end

  def test_send_message_without_parameters
    assert_raises(Rsmq::Error) { @rsmq.send_message(qname: nil, message: nil) }
  end

  def test_send_message_without_message_key
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(ArgumentError) { @rsmq.send_message(qname: @queue1[:name], messXXXage: "Hello") }
  end

  def test_send_message_number_message
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(Rsmq::Error) { @rsmq.send_message(qname: @queue1[:name], message: 123) }
  end

  def test_send_message_existing_redis_instance
    @rsmq.create_queue(qname: @queue1[:name])
    msg_id = @rsmq2.send_message(qname: @queue1[:name], message: "Hello")
    @q1m1 = {id: msg_id, message: "Hello"}
    refute_nil(msg_id)
  end

  def test_send_1000_messages_to_queue2
    @rsmq.create_queue(qname: @queue2[:name])
    messages = (0...1000).map { |i| {qname: @queue2[:name], message: "test message number:#{i}"} }
    messages.each do |msg|
      message_id = @rsmq.send_message(qname: msg[:qname], message: msg[:message])
      @q2msgs[message_id] = 1
    end

    assert_equal(1000, @q2msgs.keys.size)
  end

  def test_send_message2
    @rsmq.create_queue(qname: @queue1[:name])
    msg_id = @rsmq.send_message(qname: @queue1[:name], message: "World")
    @q1m2 = {id: msg_id, message: "World"}
    refute_nil(msg_id)
  end

  def test_receive_message1
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m1 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Hello"), message: "Hello"}
    msg = @rsmq2.receive_message(qname: @queue1[:name])
    assert_equal(@q1m1[:id], msg[:id])
  end

  def test_receive_message2
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m2 = {id: @rsmq.send_message(qname: @queue1[:name], message: "World"), message: "World"}
    msg = @rsmq.receive_message(qname: @queue1[:name])
    assert_equal(@q1m2[:id], msg[:id])
  end

  def test_send_message3
    @rsmq.create_queue(qname: @queue1[:name])
    msg_id = @rsmq.send_message(qname: @queue1[:name], message: "Booo!!")
    @q1m3 = {id: msg_id, message: "Booo!!"}
    refute_nil(msg_id)
  end

  def test_pop_message3
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m3 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Booo!!"), message: "Booo!!"}
    msg = @rsmq.pop_message(qname: @queue1[:name])
    assert_equal(@q1m3[:id], msg[:id])
  end

  def test_pop_message_no_message
    @rsmq.create_queue(qname: @queue1[:name])
    msg = @rsmq.pop_message(qname: @queue1[:name])
    assert_nil(msg[:id])
  end

  def test_set_visibility_timeout_non_existing_message
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises do
      @rsmq.change_message_visibility(
        qname: @queue1[:name],
        id: "abcdefghij0123456789ab",
        vt: 10
      )
    end
  end

  def test_set_visibility_timeout_message2
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m2 = {id: @rsmq.send_message(qname: @queue1[:name], message: "World"), message: "World"}
    result = @rsmq.change_message_visibility(qname: @queue1[:name], id: @q1m2[:id], vt: 10)
    assert_equal(1, result)
  end

  def test_receive_message_no_message
    @rsmq.create_queue(qname: @queue1[:name])
    msg = @rsmq.receive_message(qname: @queue1[:name])
    assert_nil(msg[:id])
  end

  def test_set_visibility_timeout_message2_to_0
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m2 = {id: @rsmq.send_message(qname: @queue1[:name], message: "World"), message: "World"}
    result = @rsmq.change_message_visibility(qname: @queue1[:name], id: @q1m2[:id], vt: 0)
    assert_equal(1, result)
  end

  def test_receive_message2_again
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m2 = {id: @rsmq.send_message(qname: @queue1[:name], message: "World"), message: "World"}
    msg = @rsmq.receive_message(qname: @queue1[:name])
    assert_equal(@q1m2[:id], msg[:id])
  end

  def test_receive_message_no_message_again
    @rsmq.create_queue(qname: @queue1[:name])
    msg = @rsmq.receive_message(qname: @queue1[:name])
    assert_nil(msg[:id])
  end

  def test_delete_message_no_id
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(Rsmq::Error) { @rsmq.delete_message(qname: @queue1[:name]) }
  end

  def test_delete_message_invalid_id
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(Rsmq::Error) { @rsmq.delete_message(qname: @queue1[:name], id: "sdafsdf") }
  end

  def test_delete_message1
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m1 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Hello"), message: "Hello"}
    assert_equal(1, @rsmq.delete_message(qname: @queue1[:name], id: @q1m1[:id]))
  end

  def test_delete_message1_again
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m1 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Hello"), message: "Hello"}
    @rsmq.delete_message(qname: @queue1[:name], id: @q1m1[:id])
    assert_equal(0, @rsmq.delete_message(qname: @queue1[:name], id: @q1m1[:id]))
  end

  def test_set_visibility_timeout_deleted_message
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m1 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Hello"), message: "Hello"}
    @rsmq.delete_message(qname: @queue1[:name], id: @q1m1[:id])
    result = @rsmq.change_message_visibility(qname: @queue1[:name], id: @q1m1[:id], vt: 10)
    assert_equal(0, result)
  end

  def test_send_long_message
    @rsmq.create_queue(qname: @queue1[:name])
    text = looong_string
    assert_raises(Rsmq::Error) { @rsmq.send_message(qname: @queue1[:name], message: text) }
  end

  def test_receive_1000_messages_and_delete_500
    @rsmq.create_queue(qname: @queue2[:name])

    # Send 1000 messages
    messages = (0...1000).map { |i| {qname: @queue2[:name], message: "test message number:#{i}"} }
    messages.each do |msg|
      message_id = @rsmq.send_message(qname: msg[:qname], message: msg[:message])
      @q2msgs[message_id] = 1
    end

    assert_equal(1000, @q2msgs.size, "Not all messages were sent")

    received_msgs = 1000.times.map { @rsmq.receive_message(qname: @queue2[:name], vt: 0) }.compact

    assert_equal(1000, received_msgs.size, "Not all messages were received")

    delete_msgs = received_msgs.take(500)

    assert_equal(500, delete_msgs.size, "Not all even-numbered messages were selected for deletion")

    delete_msgs.each do |msg|
      @q2msgs.delete(msg[:id])
      assert_equal(1, @rsmq.delete_message(qname: @queue2[:name], id: msg[:id]))
    end

    assert_equal(500, @q2msgs.keys.size, "Not all messages were deleted")
  end

  def test_queue2_attributes
    @rsmq.create_queue(qname: @queue2[:name])
    messages = (0...1500).map { |i| {qname: @queue2[:name], message: "test message number:#{i}"} }
    messages.each do |msg|
      message_id = @rsmq.send_message(qname: msg[:qname], message: msg[:message])
      @q2msgs[message_id] = 1
    end

    1000.times.map { @rsmq.receive_message(qname: @queue2[:name], vt: 0) }

    attrs = @rsmq.get_queue_attributes(qname: @queue2[:name])
    assert_equal(1000, attrs["totalrecv"].to_i)
    assert_equal(1500, attrs["totalsent"].to_i)
  end

  def test_set_unlimited_maxsize_queue2
    @rsmq.create_queue(qname: @queue2[:name])
    attrs = @rsmq.set_queue_attributes(qname: @queue2[:name], delay: 0, vt: 30, maxsize: -1)
    assert_equal(-1, attrs["maxsize"].to_i)
  end

  def test_send_receive_long_message_unlimited
    @rsmq.create_queue(qname: @queue2[:name], maxsize: -1)
    longmsg = looong_string
    msg_id = @rsmq.send_message(qname: @queue2[:name], message: longmsg)
    msg = @rsmq.receive_message(qname: @queue2[:name])
    assert_equal(longmsg, msg[:message])
    assert_equal(msg_id, msg[:id])
  end

  def test_send_another_message_queue1
    @rsmq.create_queue(qname: @queue1[:name])
    assert(@rsmq.send_message(qname: @queue1[:name], message: "Another World"))
  end

  def test_wait_realtime
    sleep(0.1)
  end

  def test_send_another_message_queue1_again
    @rsmq.create_queue(qname: @queue1[:name])
    assert(@rsmq.send_message(qname: @queue1[:name], message: "Another World"))
  end

  def test_wait_realtime_again
    sleep(0.1)
  end

  def test_queue_properties_after_receiving_messages
    @rsmq.create_queue(qname: @queue1[:name])

    # Send messages with a delay
    @q1m1 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Hello", delay: 1), message: "Hello"}
    @q1m2 = {id: @rsmq.send_message(qname: @queue1[:name], message: "World", delay: 1), message: "World"}

    # Immediately check queue attributes
    attrs = @rsmq.get_queue_attributes(qname: @queue1[:name])
    assert_equal(2, attrs["msgs"].to_i)
    assert_equal(2, attrs["hiddenmsgs"].to_i)

    # Wait for the delay period to expire
    sleep(1.1)

    # Receive the messages with a visibility timeout (vt)
    @rsmq.receive_message(qname: @queue1[:name], vt: 0)
    @rsmq.receive_message(qname: @queue1[:name], vt: 0)

    # Check queue attributes again after delay
    attrs = @rsmq.get_queue_attributes(qname: @queue1[:name])
    assert_equal(2, attrs["msgs"].to_i)
    assert_equal(0, attrs["hiddenmsgs"].to_i)
  end

  def test_queue_properties_after_popping_message3
    @rsmq.create_queue(qname: @queue1[:name])
    @q1m3 = {id: @rsmq.send_message(qname: @queue1[:name], message: "Booo!!"), message: "Booo!!"}
    @rsmq.pop_message(qname: @queue1[:name])
    attrs = @rsmq.get_queue_attributes(qname: @queue1[:name])
    assert_equal(0, attrs["msgs"].to_i)
    assert_equal(1, attrs["totalrecv"].to_i)
  end

  def test_create_same_queue_again
    @rsmq.create_queue(qname: @queue1[:name])
    assert_raises(Rsmq::Error) { @rsmq.create_queue(qname: @queue1[:name]) }
  end

  def test_check_queue1_length_2
    @rsmq.create_queue(qname: @queue1[:name])
    @rsmq.send_message(qname: @queue1[:name], message: "Hello")
    @rsmq.send_message(qname: @queue1[:name], message: "World")
    attrs = @rsmq.get_queue_attributes(qname: @queue1[:name])
    assert_equal(2, attrs["msgs"].to_i)
  end

  def test_check_queue1_length_3
    @rsmq.create_queue(qname: @queue1[:name])
    @rsmq.send_message(qname: @queue1[:name], message: "Hello")
    @rsmq.send_message(qname: @queue1[:name], message: "World")
    @rsmq.send_message(qname: @queue1[:name], message: "Another World")
    attrs = @rsmq.get_queue_attributes(qname: @queue1[:name])
    assert_equal(3, attrs["msgs"].to_i)
  end
end
