
Gem::Specification.new do |spec|
  spec.name          = "embulk-output-mailchimp"
  spec.version       = "0.1.0"
  spec.authors       = ["takkanm"]
  spec.summary       = "Mailchimp output plugin for Embulk"
  spec.description   = "Dumps records to Mailchimp."
  spec.email         = ["takkanm@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/takkanm/embulk-output-mailchimp"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_dependency 'mailchimp-api', ['~> 2.0.6']
  spec.add_dependency 'perfect_retry', "~> 0.3"

  spec.add_development_dependency 'embulk', ['~> 0.7.9']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
