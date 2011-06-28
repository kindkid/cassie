module Cassie
  class RowStream < AbstractStream
    def initialize(options={})
      super(options)
      @cf = options[:cf] || raise(ArgumentError, "missing :cf argument")
    end

    protected

    # Override this in your sub-class. Don't call super.
    # get_connection do |connection|
    #   connection.get(...)
    # end
    def get_connection
      raise "Implement this in your sub-class. Don't call super."
    end

    # Override this in your sub-class. Default returns [key, row].
    def build_object(key)
      get_connection do |connection|
        return [key, connection.get(@cf, key)]
      end
    end

    private

    def fetch_chunk(options={})
      result = nil
      get_connection do |connection|
        result = connection.get_range(@cf,
          :start => options[:start],
          :finish => options[:finish],
          :count => options[:count])
      end
      result
    end

    def key_of(key)
      key
    end

  end
end
