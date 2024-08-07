require "redis"
require "securerandom"
require "timeout"

require_relative "../lib/rsmq-rb/rsmq"

class IntegrationServer
  def initialize
    @rsmq = Rsmq.new(host: "localhost", port: 6379, ns: "acme_widgets")

    @transaction_queue = "widget_transaction_queue"
    @log_queue = "system_log_queue"
    @notification_queue = "system_notification_queue"
    @supplier_order_queue = "supplier_order_queue"
    @supplier_response_queue = "supplier_response_queue"
    @broadcast_queue = "system_broadcast_queue"
    @dead_letter_queue = "dead_letter_queue"

    # Buffer for widget transaction aggregation
    @transaction_buffer = []
    # Threshold for creating supplier orders
    @transaction_threshold = 3

    # Start by cleaning up and resetting all queues
    reset_queues
  end

  def reset_queues
    queue_names = [
      @transaction_queue,
      @log_queue,
      @notification_queue,
      @supplier_order_queue,
      @supplier_response_queue,
      @broadcast_queue,
      @dead_letter_queue
    ]
    queue_names.each { |q| delete_queue_if_exists(q) }

    create_queues
  end

  def delete_queue_if_exists(qname)
    begin
      @rsmq.delete_queue(qname: qname)
    rescue Rsmq::Error => e
      puts("Queue not found or not deleted: #{qname}. Error: #{e.message}")
    end
  end

  def create_queues
    @rsmq.create_queue(qname: @transaction_queue)
    @rsmq.create_queue(qname: @log_queue)
    @rsmq.create_queue(qname: @notification_queue)
    @rsmq.create_queue(qname: @supplier_order_queue)
    @rsmq.create_queue(qname: @supplier_response_queue)
    @rsmq.create_queue(qname: @broadcast_queue)
    @rsmq.create_queue(qname: @dead_letter_queue)
  end

  def process_messages
    loop do
      process_transactions
      process_supplier_orders
      process_logs
      process_notifications
      broadcast_system_notifications

      # Simulating message polling every second
      sleep(1)
    end
  end

  # Processing transactions that are stored in the transaction queue
  def process_transactions
    msg = @rsmq.receive_message(qname: @transaction_queue)
    if msg[:message]
      puts("Processing transaction: #{msg[:message]}")
      @transaction_buffer << msg[:message]
      log_transaction(msg[:message])
      create_supplier_order if @transaction_buffer.size >= @transaction_threshold
    else
      puts("No transactions to process.")
    end
  end

  # Create a supplier order once the transaction threshold is met
  def create_supplier_order
    @transaction_buffer.each do |transaction|
      supplier_order_id = SecureRandom.hex(8)
      puts("Creating supplier order for transaction: #{transaction}, Order ID: #{supplier_order_id}")
      @rsmq.send_message(
        qname: @supplier_order_queue,
        message: "Supplier Order #{supplier_order_id}: Restock needed materials"
      )
    end

    @transaction_buffer.clear
  end

  # Processing supplier orders
  def process_supplier_orders
    order_msg = @rsmq.receive_message(qname: @supplier_order_queue)
    if order_msg[:message]
      order_id = order_msg[:message].match(/Supplier Order (\w+)/)[1]
      puts("Processing supplier order: #{order_msg[:message]}")
      # Send a response after processing the supplier order
      @rsmq.send_message(qname: @supplier_response_queue, message: "Supplier Order #{order_id}: Processed")
    end

    # Receiving the supplier's response
    supplier_response = @rsmq.receive_message(qname: @supplier_response_queue)
    puts("Received supplier response: #{supplier_response[:message]}") if supplier_response[:message]
  end

  # Processing system logs
  def process_logs
    msg = @rsmq.receive_message(qname: @log_queue)
    puts("Processing system log: #{msg[:message]}") if msg[:message]
  end

  # Processing system notifications
  def process_notifications
    msg = @rsmq.receive_message(qname: @notification_queue)
    puts("Processing system notification: #{msg[:message]}") if msg[:message]
  end

  # Handling system broadcasts (alerts/maintenance)
  def broadcast_system_notifications
    msg = @rsmq.receive_message(qname: @broadcast_queue)
    puts("Broadcasting system-wide notification: #{msg[:message]}") if msg[:message]
  end

  # Log the transactions as part of the system logs
  def log_transaction(transaction)
    @rsmq.send_message(qname: @log_queue, message: "Log: #{transaction} processed.")
  end

  # Handle errors and move unprocessed messages to the dead letter queue
  def handle_error(message)
    puts("Error processing message: #{message}, moving to dead letter queue.")
    @rsmq.send_message(qname: @dead_letter_queue, message: message)
  end

  # Simulate client activity that generates transactions and logs
  def simulate_client_activity
    5.times do |i|
      widget_id = SecureRandom.hex(4)
      @rsmq.send_message(qname: @transaction_queue, message: "Widget #{widget_id}: Produced")
      sleep(0.5)
      @rsmq.send_message(qname: @log_queue, message: "Log entry #{i + 1}: System operational")
      @rsmq.send_message(qname: @notification_queue, message: "Notification #{i + 1}: New supplier order required")
    end

    # Simulating a system-wide broadcast (e.g., maintenance notice)
    @rsmq.send_message(qname: @broadcast_queue, message: "System-wide maintenance scheduled for 2AM.")
  end

  def run_with_timeout(seconds)
    begin
      Timeout.timeout(seconds) do
        Thread.new { process_messages }
        simulate_client_activity
        sleep(1)
      end

    rescue Timeout::Error
      puts("Shutting down after #{seconds} seconds.")
    end
  end
end

server = IntegrationServer.new
server.run_with_timeout(10)

server.reset_queues
puts("Server has shut down and queues have been reset.")
