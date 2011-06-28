module Cassie
  class AbstractStream
    def initialize(options = {})
      @chunk_size = options[:chunk_size] || 100
      @start = options[:start]
      @finish = options[:finish]
      @frontier = nil
      @chunk = nil
      @drained = false
      @offset = 0
    end

    def get_next
      return nil if @drained
      _fetch_chunk if @chunk.nil?
      if chunk_drained?
        _fetch_chunk
        if chunk_drained?
          @frontier = @finish
          @drained = true
          return nil
        end
      end
      result = @chunk[@offset]
      @offset += 1
      @frontier = key_of(result)
      return result
    end

    def each(&callback)
      while(n = get_next)
        callback.call(n)
      end
    end

    protected

    # Over-ride this in your sub-class
    def key_of(entry)
      entry
    end

    def fetch_chunk(options={})
      raise "Implement this method when you extend Cassie::AbstractStream."
      # start_key         = options[:start]
      # final_key         = options[:finish]
      # limit_result_size = options[:count]
    end

    # Over-ride this in your sub-class
    def build_object(entry)
      entry
    end

    private

    def _fetch_chunk
      @chunk = fetch_chunk(:start => @frontier || @start,
                           :finish => @finish,
                           :count => @chunk_size).to_a
      @offset = @frontier.nil? ? 0 : 1
    end

    def chunk_drained?
      @offset >= @chunk.size
    end
  end
end