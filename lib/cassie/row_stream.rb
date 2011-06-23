module Cassie
  class RowStream < AbstractStream
    def initialize(options={})
      super(options)
      @cf = options[:cf] || raise(ArgumentError, "missing :cf argument")
    end

    protected

    # Override this in your sub-class. Don't call super.
    def connection
      raise "Implement this in your sub-class. Don't call super."
    end

    # Override this in your sub-class. Default returns [key, row].
    def build_object(entry)
      [entry, connection.get(@cf, entry)]
    end

    private

    def fetch_chunk(options={})
      connection.get_range(@cf, :start => options[:start],
                                :finish => options[:finish],
                                :count => options[:count])
    end

    def key_of(entry)
      entry
    end

  end
end
