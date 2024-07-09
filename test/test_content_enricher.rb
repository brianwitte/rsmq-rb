require 'minitest/autorun'
require 'redis'
require 'securerandom'
require 'pry-byebug'
require_relative '../lib/rsmq-rb/rsmq'

class TestContentEnricher < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name = 'content_enricher_queue'
    @rsmq.create_queue(qname: @queue_name)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @redis.flushdb
    @redis.close
  end

  def test_enrich_message
    original_message = "user_id:12345"
    @rsmq.send_message(qname: @queue_name, message: original_message)
    enriched_message = enrich_message(@rsmq.receive_message(qname: @queue_name)[:message])
    expected_message = "user_id:12345;name:John Doe;email:john@example.com"

    assert_equal(expected_message, enriched_message)
  end

  def enrich_message(message)
    user_details = {name: 'John Doe', email: 'john@example.com'}
    "#{message};" + user_details.map { |k, v| "#{k}:#{v}" }.join(';')
  end
end
