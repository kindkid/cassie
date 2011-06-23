# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "cassie/version"

Gem::Specification.new do |s|
  s.name        = "cassie"
  s.version     = Cassie::VERSION
  s.authors     = ["Chris Johnson"]
  s.email       = ["chris@codefriend.org"]
  s.homepage    = ""
  s.summary     = "A friendlier Cassandra"
  s.description = "Convenience methods for working with Cassandra"

  s.rubyforge_project = "cassie"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  s.add_development_dependency 'rspec', '>= 2.5.0'
  s.add_development_dependency 'simplecov', '>= 0.4.0'
  s.add_development_dependency('rb-fsevent', '>= 0.4.0') if RUBY_PLATFORM =~ /darwin/i
  s.add_development_dependency 'guard', '>= 0.3.4'
  s.add_development_dependency 'guard-bundler', '>= 0.1.2'
  s.add_development_dependency 'guard-rspec', '>= 0.3.1'
  s.add_development_dependency 'guard-ego', '>= 0.0.1'
end
