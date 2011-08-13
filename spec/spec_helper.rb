require 'simplecov'
SimpleCov.start

require 'rubygems'
require 'bundler'

Bundler.require(:default, :test, :development)

Cassandra = Object.new
$connection = Object.new
def $connection.keyspaces; ['system','cassie_gem']; end
def $connection.keyspace=(ks); end
def $connection.disconnect!; end
def Cassandra.new(*args); $connection; end
