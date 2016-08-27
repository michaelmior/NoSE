require 'scrutinizer/ocular'

SimpleCov.formatters = [
  SimpleCov::Formatter::HTMLFormatter,
]

SimpleCov.formatters << Scrutinizer::Ocular::UploadFormatter \
  if Scrutinizer::Ocular.should_run?

SimpleCov.start do
  add_filter '/spec/'
  add_filter '/vendor/'
end
