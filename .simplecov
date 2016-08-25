require 'scrutinizer/ocular'

SimpleCov.formatters = [
  SimpleCov::Formatter::HTMLFormatter,
  Scrutinizer::Ocular::UploadFormatter
]

SimpleCov.start do
  add_filter '/spec/'
  add_filter '/vendor/'
end
