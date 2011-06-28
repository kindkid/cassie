module Cassie
  class ColumnStream < AbstractStream
    def initialize(options={})
      super(options)
      @cf = options[:cf] || raise(ArgumentError, "missing :cf argument")
      @key = options[:key] || raise(ArgumentError,"missing :key argument")
    end

    protected

    # Override this in your sub-class. Don't call super.
    # get_connection do |connection|
    #   connection.get(...)
    # end
    def get_connection(&callback)
      raise "Implement this in your sub-class. Don't call super."
    end

    # Override this in your sub-class. Default returns [column, value].
    def build_object(entry)
      entry
    end

    private

    def fetch_chunk(options={})
      result = nil
      get_connection do |connection|
        result = connection.get(@cf, @key,
          :start => options[:start],
          :finish => options[:finish],
          :count => options[:count])
      end
      result
    end

    def key_of(entry)
      entry.first
    end

  end
end
