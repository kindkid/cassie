Cassie
======
A friendlier face for [Cassandra](http://github.com/fauna/cassandra).

Usage
=====
    require 'cassie'
    require 'cassandra/0.8'
    require 'em-synchrony'

    CASSIE = Cassie.new(:keyspace => 'example', :servers => '10.0.0.1:9160')

    CASSIE.now do |cassie|
       # This block runs synchronously,
       # All calls to cassie are also synchronous
       cassie.insert #blah blah blah
    end

    CASSIE.queue do |c|
       # This block runs synchronously,
       # All calls to cassie are asynchronous (immediate return value is simply: true)
       cassie.insert #blah blah blah
    end

    CASSIE.start_processing! # begin sending queued calls to cassandra

    CASSIE.stop_processing! # stop sending queued calls to cassandra

    CASSIE.fiber do |c|
       # This block runs asynchronously in a fiber
       # But WITHIN the block, all calls to cassie look synchronous.
       c.insert #blah blah blah
    end
