require "cassie/version"
require "cassie/cassandra_proxy"
require 'thread'

class Cassie

  DEFAULT_RETRIES = 2
  DEFAULT_TIMEOUT = 3 # seconds
  DEFAULT_CONNECT_TIMEOUT = 3 # seconds
  DEFAULT_INTERVAL = 0.1 # seconds
  DEFAULT_POOL_SIZE = 10

  attr_reader :keyspace  # The name of the keyspace.
  attr_reader :servers   # An array of ip:host strings.
  attr_reader :options   # Passed to the Cassandra initializer.
  attr_reader :pool_size # Number of cassandra connections in the pool.
  attr_reader :interval  # Seconds per queue-processor cycle.

  def initialize(opts={})
    @keyspace = (opts[:keyspace] || raise("missing param: keyspace")).to_s
    @servers = Array(opts[:servers] || '127.0.0.1:9160')
    @options = {}
    @options[:retries] = (opts[:retries] || DEFAULT_RETRIES).to_i
    @options[:timeout] = (opts[:timeout] || DEFAULT_TIMEOUT).to_f
    @options[:connect_timeout] = 
      (opts[:connect_timeout] || DEFAULT_CONNECT_TIMEOUT).to_f
    @pool_size = (opts[:pool_size] || DEFAULT_POOL_SIZE).to_i
    @interval = (opts[:interval] || DEFAULT_INTERVAL).to_f
    @started = false
    @processing = false
    @timer = nil
    @queue = nil
    @pool = nil
    @proxy = nil
    @healthy = nil
    @semaphore = Mutex.new
  end

  # If last connection succeeded, true. If failed, false. If unattempted, nil.
  # The result is only losely defined, due to asynchronous nature of operations.
  def healthy?
    @healthy
  end

  # Runs the block synchronously.
  # Passes the block an open Cassandra connection, disconnected afterwards.
  def now(&callback)
    connection = nil
    connection = new_connection
    callback.call(connection)
  rescue
    @healthy = false
    raise
  ensure
    connection.disconnect! unless connection.nil?
  end

  # Runs the block asynchronously, in a fiber (yielding and resuming as needed).
  # Passes the block a pooled Cassandra connection, returned afterwards.
  def fiber(&callback)
    EM.synchrony do
      begin
        callback.call(pooled_connection)
        @healthy = true
      rescue
        @healthy = false
        raise
      end
    end
  end

  # Runs the block synchronously, but...
  # Passes the block a proxied Cassandra connection.
  # Methods called on the proxy run asynchronously, and always return true.
  #
  # Use this for maximum throughput if you don't care about return values.
  #
  # See: start_processing! and stop_processing!
  def queue(&callback)
    callback.call(cassandra_proxy)
  end

  # Accepts a cassandra connection (in a "now" or "fiber" block).
  # Yields [column, value] pairs for a row in the column family.
  #
  # Use this method when you don't know how many columns to expect.
  def each_column(cassandra, cf, key, opts={})
    start_column = opts[:start] || ''
    prev_column = nil
    while start_column != prev_column
      start_column = prev_column
      adjusted_opts = opts.merge(:start => start_column || opts[:start])
      chunk = cassandra.get(cf, key, adjusted_opts)
      chunk.each do |column, value|
        #raise "hell" if (column <=> start_column) < 0
        next if start_column == column
        yield column, value
        prev_column = column
      end
    end
    true
  end

  # Watch for and process queued cassandra calls. Continue until told to stop.
  def start_processing!
    return true if @started
    synchronize {
      return true if @started
      @started = true
    }
    EM.next_tick do
      process!
    end
    true
  end

  # Stop watching for queued cassandra calls.
  def stop_processing!
    return true unless @started
    synchronize {
      return true unless @started
      @started = false
    }
    if timer = @timer
      EM.cancel_timer(timer) rescue nil
      @timer = nil
    end
    true
  end

  private

  def synchronize
    @semaphore.synchronize do
      yield
    end
  end

  def reschedule_process!
    if @started
      if timer = @timer
        EM.cancel_timer(timer) rescue nil
      end
      @timer = EM.add_timer(PROCESS_INTERVAL) do
        process!
      end
    end
  end

  def process!
    synchronize {
      return if @processing
      @processing = true
    }
    @timer = nil
    fiber do |cassandra|
      until get_queue.empty?
        get_queue.pop do |method, callback, *args|
          cassandra.send(method, *args, &callback)
        end
      end
      synchronize { @processing = false }
      reschedule_process!
    end
  rescue
    synchronize { @processing = false }
    raise
  end

  def get_queue
    return @queue if @queue
    synchronize {
      return @queue ||= EventMachine::Queue.new
    }
  end

  def pooled_connection
    return @pool if @pool
    synchronize {
      return @pool ||= EM::Synchrony::ConnectionPool.new(:size => @pool_size) do
        new_connection(:transport => Thrift::EventMachineTransport,
                       :transport_wrapper => nil)
      end
    }
  end

  def new_connection(opts={})
    connection = Cassandra.new('system', @servers, @options.merge(opts))
    check_keyspace!(connection)
    connection.keyspace = @keyspace
    @healthy = true
    connection
  rescue
    @healthy = false
    raise
  end

  def cassandra_proxy
    return @proxy if @proxy
    synchronize {
      return @proxy ||= CassandraProxy.new(self)
    }
  end

  def check_keyspace!(connection)
    unless Array(connection.keyspaces).map(&:to_s).include?(@keyspace)
      raise("Keyspace '#{@keyspace}' not found on #{@servers}")
    end
  end
end

true