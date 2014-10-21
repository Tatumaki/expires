# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'expires/version'

Gem::Specification.new do |spec|
  spec.name          = "expires"
  spec.version       = Expires::VERSION
  spec.authors       = ["Tatumaki"]
  spec.email         = ["Tatumaki.x.euphoric@gmail.com"]
  spec.summary       = %q{Expires gives a valiable with limited time.}
  spec.description   = %q{}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'color_print'
  spec.add_dependency 'sqlite3'

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
end
