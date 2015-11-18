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
