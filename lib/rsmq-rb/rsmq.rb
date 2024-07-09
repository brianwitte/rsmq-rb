# SPDX-License-Identifier: MIT

require "redis"

class Rsmq
  class Error < StandardError
  end

  attr_reader :config, :redis

  POSSIBLE_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  ID_LENGTH = 22

  ERRORS = {
    no_attribute_supplied: "No attribute was supplied",
    missing_parameter: "No %<item>s supplied",
    invalid_format: "Invalid %<item>s format",
    invalid_value: "%<item>s must be between %<min>s and %<max>s",
    message_not_string: "Message must be a string",
    message_too_long: "Message too long",
    queue_not_found: "Queue not found",
    queue_exists: "Queue already exists"
  }.freeze

  def initialize(host: "127.0.0.1", port: 6379, options: {}, client: nil, ns: "rsmq", realtime: false, password: nil)
    @config = {
      host:,
      port:,
      options:,
      client:,
      ns:,
      realtime:,
      password:
    }
    @redis = client || Redis.new(host:, port:, password:, **options)
    init_script
  end

def create_queue(qname:, vt: 30, delay: 0, maxsize: 65_536, ttl: nil)
  validate_queue_name(qname)
  validate_vt(vt)
  validate_delay(delay)
  validate_maxsize(maxsize)

  raise Error, ERRORS[:queue_exists] if queue_exists?(qname)

  result = @redis.multi do |pipeline|
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "vt", vt)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "delay", delay)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "maxsize", maxsize)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "created", Time.now.to_i)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "modified", Time.now.to_i)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "msgs", 0)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "totalrecv", 0)
    pipeline.hset("#{config[:ns]}:#{qname}:Q", "hiddenmsgs", 0)
    pipeline.expire("#{config[:ns]}:#{qname}:Q", ttl) if ttl
  end

  raise Error, ERRORS[:queue_exists] unless result

  1
end


  def list_queues
    @redis.keys("#{config[:ns]}:*:Q").map { |k| k.split(":")[1] }
  end

  def delete_message(qname:, id: nil)
    raise Rsmq::Error, format(ERRORS[:missing_parameter], item: "qname") if qname.nil? || qname.empty?
    raise Rsmq::Error, format(ERRORS[:missing_parameter], item: "id") if id.nil? || id.empty?

    validate_queue_name(qname)
    validate_message_id(id)

    result = @redis.zrem("#{config[:ns]}:#{qname}", id)
    if result == 1
      @redis.hdel("#{config[:ns]}:#{qname}:Q", id, "#{id}:rc", "#{id}:fr")
      @redis.hincrby("#{config[:ns]}:#{qname}:Q", "msgs", -1)
    end

    result ? 1 : 0
  rescue ArgumentError => e
    raise Rsmq::Error, e.message
  end

  def delete_queue(qname:)
    validate_queue_name(qname)
    @redis.del("#{config[:ns]}:#{qname}:Q")
    1
  end

  def get_queue_attributes(qname:)
    validate_queue_name(qname)
    result = @redis.hgetall("#{config[:ns]}:#{qname}:Q")
    raise Error, ERRORS[:queue_not_found] if result.empty?

    result
  end

  def set_queue_attributes(qname:, vt: nil, delay: nil, maxsize: nil)
    validate_queue_name(qname)
    raise Rsmq::Error, ERRORS[:queue_not_found] unless queue_exists?(qname)

    validate_vt(vt) if vt
    validate_delay(delay) if delay
    validate_maxsize(maxsize) if maxsize

    raise Error, ERRORS[:no_attribute_supplied] if [vt, delay, maxsize].all?(&:nil?)

    attributes = {}
    attributes["vt"] = vt if vt
    attributes["delay"] = delay if delay
    attributes["maxsize"] = maxsize if maxsize
    attributes["modified"] = Time.now.to_i

    @redis.hmset("#{config[:ns]}:#{qname}:Q", *attributes.flatten) unless attributes.empty?
    get_queue_attributes(qname: qname)
  end

  def send_message(qname:, message: nil, delay: 0)
    raise Rsmq::Error, format(ERRORS[:missing_parameter], item: "qname") if qname.nil?
    raise Rsmq::Error, format(ERRORS[:missing_parameter], item: "message") if message.nil?

    queue_attributes = get_queue_attributes(qname: qname)

    validate_queue_name(qname)
    raise Error, ERRORS[:message_not_string] unless message.is_a?(String)
    if message.size > 65_536 &&
        queue_attributes["maxsize"].to_i != -1
      raise Error, ERRORS[:message_too_long]
    end

    raise Rsmq::Error, ERRORS[:queue_not_found] unless queue_exists?(qname)

    delay ||= queue_attributes["delay"].to_i
    validate_delay(delay)

    id = make_id(ID_LENGTH)
    key = "#{config[:ns]}:#{qname}"

    @redis.multi do |pipeline|
      pipeline.zadd(key, Time.now.to_i + delay, id)
      pipeline.hset("#{key}:Q", id, message)
      pipeline.hincrby("#{key}:Q", "totalsent", 1)
      pipeline.hincrby("#{key}:Q", "msgs", 1)
      pipeline.hincrby("#{key}:Q", "hiddenmsgs", 1) if delay > 0
    end

    if config[:realtime]
      queue_length = @redis.zcard(key)
      @redis.publish("#{config[:ns]}rt:#{qname}", queue_length)
    end

    id
  end

  def receive_message(qname:, vt: nil)
    validate_queue_name(qname)

    raise Error, ERRORS[:queue_not_found] unless queue_exists?(qname)

    vt ||= @redis.hget("#{config[:ns]}:#{qname}:Q", "vt").to_i
    now = Time.now.to_i
    msg = @redis.zrangebyscore("#{config[:ns]}:#{qname}", "-inf", now, limit: [0, 1]).first
    return {} unless msg

    @redis.multi do |pipeline|
      pipeline.zadd("#{config[:ns]}:#{qname}", now + vt, msg)
      pipeline.hincrby("#{config[:ns]}:#{qname}:Q", "totalrecv", 1)
      pipeline.hincrby("#{config[:ns]}:#{qname}:Q", "#{msg}:rc", 1)
      pipeline.hincrby("#{config[:ns]}:#{qname}:Q", "hiddenmsgs", -1)
    end

    {
      id: msg,
      message: @redis.hget("#{config[:ns]}:#{qname}:Q", msg),
      sent: @redis.hget("#{config[:ns]}:#{msg}:Q", "created").to_i,
      fr: @redis.hget("#{config[:ns]}:#{msg}:Q", "fr").to_i,
      rc: @redis.hget("#{config[:ns]}:#{qname}:Q", "#{msg}:rc").to_i
    }
  end

  def pop_message(qname:)
    validate_queue_name(qname)

    now = Time.now.to_i
    msg = @redis.evalsha(@pop_message_sha1, keys: ["#{config[:ns]}:#{qname}", now])
    return {} unless msg.any?

    @redis.hincrby("#{config[:ns]}:#{qname}:Q", "msgs", -1)

    {
      id: msg[0],
      message: msg[1],
      rc: msg[2].to_i,
      fr: msg[3].to_i,
      sent: @redis.hget("#{config[:ns]}:#{qname}:Q", "created").to_i
    }
  end

  def change_message_visibility(qname:, id:, vt:)
    validate_queue_name(qname)
    validate_message_id(id)
    validate_vt(vt)

    raise Error, ERRORS[:queue_not_found] unless queue_exists?(qname)
    raise Error, "Message does not exist" unless message_exists?(qname, id)

    now = Time.now.to_i
    result = @redis.evalsha(@change_message_visibility_sha1, keys: ["#{config[:ns]}:#{qname}", id, now + vt])
    result.to_i
  end

  def quit
    @redis.close
  end

  private

  SCRIPT_POPMESSAGE = "local msg = redis.call(\"ZRANGEBYSCORE\", KEYS[1], \"-inf\", KEYS[2], \"LIMIT\", \"0\", \"1\") if #msg == 0 then return {} end redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", \"totalrecv\", 1) local mbody = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1]) local rc = redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", msg[1] .. \":rc\", 1) local o = {msg[1], mbody, rc} if rc==1 then table.insert(o, KEYS[2]) else local fr = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1] .. \":fr\") table.insert(o, fr) end redis.call(\"ZREM\", KEYS[1], msg[1]) redis.call(\"HDEL\", KEYS[1] .. \":Q\", msg[1], msg[1] .. \":rc\", msg[1] .. \":fr\") return o"
  SCRIPT_RECEIVEMESSAGE = "local msg = redis.call(\"ZRANGEBYSCORE\", KEYS[1], \"-inf\", KEYS[2], \"LIMIT\", \"0\", \"1\") if #msg == 0 then return {} end redis.call(\"ZADD\", KEYS[1], KEYS[3], msg[1]) redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", \"totalrecv\", 1) local mbody = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1]) local rc = redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", msg[1] .. \":rc\", 1) local o = {msg[1], mbody, rc} if rc==1 then redis.call(\"HSET\", KEYS[1] .. \":Q\", msg[1] .. \":fr\", KEYS[2]) table.insert(o, KEYS[2]) else local fr = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1] .. \":fr\") table.insert(o, fr) end return o"
  SCRIPT_CHANGEMESSAGEVISIBILITY = "local msg = redis.call(\"ZSCORE\", KEYS[1], KEYS[2]) if not msg then return 0 end redis.call(\"ZADD\", KEYS[1], KEYS[3], KEYS[2]) return 1"

  def init_script
    @pop_message_sha1 = @redis.script(:load, SCRIPT_POPMESSAGE)
    @receive_message_sha1 = @redis.script(:load, SCRIPT_RECEIVEMESSAGE)
    @change_message_visibility_sha1 = @redis.script(:load, SCRIPT_CHANGEMESSAGEVISIBILITY)
  end

  def make_id(len)
    Array.new(len) { POSSIBLE_CHARS[rand(POSSIBLE_CHARS.size)] }.join
  end

  def validate_queue_name(qname)
    raise Error, format(ERRORS[:invalid_format], item: "qname") unless qname.match?(/\A[\w-]{1,160}\z/)
  end

  def validate_vt(vt)
    unless vt.is_a?(Integer) && (0..9_999_999).include?(vt)
      raise Error, format(ERRORS[:invalid_value], item: "vt", min: 0, max: 9_999_999)
    end
  end

  def validate_delay(delay)
    unless delay.is_a?(Integer) && (0..9_999_999).include?(delay)
      raise Error, format(ERRORS[:invalid_value], item: "delay", min: 0, max: 9_999_999)
    end
  end

  def validate_maxsize(maxsize)
    unless maxsize.is_a?(Integer) && (maxsize == -1 || (1024..65_536).include?(maxsize))
      raise Error, format(ERRORS[:invalid_value], item: "maxsize", min: 1024, max: 65_536)
    end
  end

  def validate_message_id(id)
    raise Error, format(ERRORS[:invalid_format], item: "id") unless id.match?(/\A[a-zA-Z0-9]{22}\z/)
  end

  def queue_exists?(qname)
    !@redis.hgetall("#{config[:ns]}:#{qname}:Q").empty?
  end

  def message_exists?(qname, id)
    !@redis.hget("#{config[:ns]}:#{qname}:Q", id).nil?
  end
end
