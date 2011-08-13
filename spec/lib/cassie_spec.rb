describe Cassie do
  before(:each) do
    @keyspace = 'cassie_gem'
    @servers = ['127.0.0.1:9160']
    @options = {:retries => 2, :timeout => 3.0, :connect_timeout => 3.0}

    @cassie = Cassie.new(@options.merge(
      :keyspace => @keyspace,
      :servers => @servers,
      :pool_size => 2,
      :interval => 0.01
    ))
  end

  after(:each) do
    @cassie.stop_processing!
  end

  describe ".healthy?" do
    context "when nothing has happened yet" do
      it "return nil" do
        @cassie.healthy?.should be_nil
      end
    end

    context "after getting a connection successfully" do
      it "returns true" do
        @cassie.now {}
        @cassie.healthy?.should be_true
      end
    end

    context "after failing to acquire a connection" do
      it "returns false" do
        @cassie.stub(:new_connection).and_raise("hell")
        expect{ @cassie.now {} }.to raise_exception
        @cassie.healthy?.should be_false
      end
    end
  end

  describe ".now(&callback)" do
    context "when it's able to create a new connection" do
      it "passes the block a new cassandra connection" do
        called = false
        Cassandra.stub(:new).and_return($connection)
        @cassie.now do |cassandra|
          cassandra.should == $connection
          called = true
        end
        called.should be_true
      end

      it "disconnects afterwards" do
        called = false
        @cassie.now do |cassandra|
          cassandra.should_receive(:disconnect!)
          called = true
        end
        called.should be_true
      end

      it "disconnects even if the block raises an error" do
        called = false
        expect do
          @cassie.now do |cassandra|
            called = true
            raise "hell"
          end
        end.to raise_exception
        called.should be_true
      end

      it "sets healthy = true" do
        @cassie.healthy?.should_not be_true
        called = false
        @cassie.now do |cassandra|
          called = true
        end
        called.should be_true
        @cassie.healthy?.should be_true
      end
    end

    context "when it fails to create a new connection" do
      it "does not call the block" do
        called = false
        @cassie.stub(:new_connection).and_raise("hell")
        expect do
          @cassie.now do |cassandra|
            called = true
          end
        end.to raise_exception

        called.should be_false
      end

      it "sets healthy = false" do
        @cassie.healthy?.should_not == false
        @cassie.stub(:new_connection).and_raise("hell")
        expect{ @cassie.now{} }.to raise_exception
        @cassie.healthy?.should == false
      end
    end
  end

  describe ".fiber(&callback)" do
    it "returns immediately, without waiting for the callback to finish"
    context "when the pooled connection works" do
      it "passes the connection to the block"
      it "appears to make synchronous calls to cassandra, WITHIN the block"
      context "and the block returns without error" do
        it "sets healthy = true"
      end
      context "but the block raises an error" do
        it "sets healthy = false"
      end
    end
    context "when the pooled connection fails" do
      it "does not bother to call the block"
      it "sets healthy = false"
    end
  end

  describe ".queue(&callback)" do
    it "returns without waiting for the cassandra methods to actually be called"
    it "queues up methods called on the cassandra proxy"
    it "calls the queued methods on a cassandra connection, later"
  end

  describe ".each_column(cassandra, cf, key, opts={})" do
    it "yields columns as [column_name, value] pairs"
    it "yields columns spanning multiple chunks"
    it "skips over columns if opts includes a :start value"
    it "stops yielding if opts includes a :stop value"
  end

  describe ".start_processing! and .stop_processing!" do
    it "ignores extra calls"
    it "continues processing until told to stop"
  end
end