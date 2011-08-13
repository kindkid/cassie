class Cassie::CassandraProxy
  def initialize(cassie)
    @cassie = cassie
  end
  
  def method_missing(method, *args, &callback)
    @cassie.send(:get_queue).push([method, callback, *args])
    true
  end
end