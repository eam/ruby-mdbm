Gem::Specification.new "ruby-mdbm" do |s|
  s.name              = "ruby-mdbm"
  s.version           = "0.0.1"
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["Evan Miller"]
  s.email             = ["evan@squareup.com"]
  s.summary           = "libmdbm bindings for ruby"
  s.description       = "MDBM is a super-fast memory-mapped key/value store. https://github.com/yahoo/mdbm"
  s.homepage          = "http://github.com/square/ruby-mdbm"
  s.files           = Dir['Rakefile', '{bin,lib,man,test,ext,spec}/**/*']
  s.extensions        = %w{ext/mdbm/extconf.rb}
end
