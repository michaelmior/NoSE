require 'codeclimate-test-reporter'

SimpleCov.formatters = [
  SimpleCov::Formatter::HTMLFormatter,
  CodeClimate::TestReporter::Formatter
]

SimpleCov.start do
  add_filter '/spec/'
  add_filter '/vendor/'

  skip_token CodeClimate::TestReporter.configuration.skip_token
end
