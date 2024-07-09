require 'minitest/autorun'
require 'redis'
require 'securerandom'
require 'pry-byebug'
require_relative '../lib/rsmq-rb/rsmq'

class TestMessageTranslator < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name = 'message_translator'
    @rsmq.create_queue(qname: @queue_name)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @redis.flushdb
    @redis.close
  end

  def test_message_translation
    sent_message = "name:John Doe;age:30"
    @rsmq.send_message(qname: @queue_name, message: sent_message)
    received_message = @rsmq.receive_message(qname: @queue_name)
    translated_message = translate_message(received_message[:message])

    expected_message = {name: 'John Doe', age: 30}
    assert_equal(expected_message, translated_message)
  end

  def translate_message(raw_message)
    data = raw_message.split(';').map { |kv| kv.split(':') }.to_h
    { name: data['name'], age: data['age'].to_i }
  end
end
