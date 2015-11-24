require "bundler/gem_tasks"

task default: :build

task 'clean' do
  Dir.glob("pkg/*.gem").each do |name|
    sh "rm #{name}"
  end
end

task 'reload' do
  spec = Bundler::GemHelper.gemspec

  sh "embulk gem uninstall #{spec.name}"
  Rake::Task['clean'].execute
  Rake::Task['build'].execute
  sh "embulk gem install pkg/#{spec.name}-#{spec.version}.gem"
end

desc 'Run tests'
task :test do
  ruby("test/run-test.rb", "--use-color=yes", "--collector=dir")
end

desc "Run tests with coverage"
task :cov do
  ENV["COVERAGE"] = "1"
  ruby("--debug", "test/run-test.rb", "--use-color=yes", "--collector=dir")
end
