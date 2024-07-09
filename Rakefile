require 'rake'
require 'rake/testtask'
require 'rake/clean'

# Clean task for cleaning up generated files
CLEAN.include('*.gem', '.gemtest')

# Define a parameterized test task
Rake::TestTask.new(:test) do |t, args|
  t.libs << 'test'
  t.verbose = true
  t.pattern = 'test/test_*.rb' # Default test pattern
end

# Default task setup to run tests
task default: :test
