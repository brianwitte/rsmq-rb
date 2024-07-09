require "minitest/autorun"
require "redis"
require "securerandom"
require "pry-byebug"

require_relative "../lib/rsmq-rb/rsmq"

class TestContentBasedRouter < Minitest::Test
  def setup
    @redis = Redis.new
    @redis.flushdb
    @rsmq = Rsmq.new(host: "127.0.0.1", port: 6379, ns: "rsmq")
    @queue_name = "content_based_router"
    @rsmq.create_queue(qname: @queue_name)
    @target_queue_admin = "admin_tasks"
    @target_queue_user = "user_tasks"
    @rsmq.create_queue(qname: @target_queue_admin)
    @rsmq.create_queue(qname: @target_queue_user)
  end

  def teardown
    @rsmq.delete_queue(qname: @queue_name)
    @rsmq.delete_queue(qname: @target_queue_admin)
    @rsmq.delete_queue(qname: @target_queue_user)
    @redis.flushdb
    @redis.close
  end

  def test_route_messages_based_on_content
    @rsmq.send_message(qname: @queue_name, message: "type:admin;action:update")
    @rsmq.send_message(qname: @queue_name, message: "type:user;action:view")
    route_messages
    admin_message = @rsmq.receive_message(qname: @target_queue_admin)
    user_message = @rsmq.receive_message(qname: @target_queue_user)

    assert_equal("type:admin;action:update", admin_message[:message])
    assert_equal("type:user;action:view", user_message[:message])
  end

  def route_messages
    while (message = @rsmq.receive_message(qname: @queue_name))[:message]
      # Debug output to trace message processing
      # puts("Processing message: #{message[:message]}")

      type_part = message[:message].split(";").find { |part| part.strip.start_with?("type") }
      if type_part
        type = type_part.split(":").last.strip
        target_queue = case type
        when "admin"
          @target_queue_admin
        when "user"
          @target_queue_user
        else
          nil
        end

        # Route message to the appropriate queue if a valid type was found
        if target_queue
          # More debug output
          # puts("Routing message to #{target_queue}: #{message[:message]}")
          @rsmq.send_message(qname: target_queue, message: message[:message])
        else
          # Handle unknown types
          # puts("Unknown type '#{type}', message not routed")
        end
      else
        # Error handling for malformed messages
        # puts("Message format incorrect or type not specified: #{message[:message]}")
      end
    end
  end
end
