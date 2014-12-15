# Namespace module for the whole project
module Sadvisor
end

require_relative 'sadvisor/util'

require_relative 'sadvisor/enumerator'
require_relative 'sadvisor/indexes'
require_relative 'sadvisor/loaders'
require_relative 'sadvisor/model'
require_relative 'sadvisor/parser'
require_relative 'sadvisor/planner'
require_relative 'sadvisor/proxy'
require_relative 'sadvisor/random'
require_relative 'sadvisor/search'
require_relative 'sadvisor/timing'
require_relative 'sadvisor/workload'

require_relative 'sadvisor/serialize'

if ENV['SADVISOR_LOG']
  require 'logging'

  logger = Logging.logger['sadvisor']
  logger.level = ENV['SADVISOR_LOG'].downcase.to_sym
  logger.add_appenders Logging.appenders.stderr
  logger = nil # rubocop:disable Lint/UselessAssignment
end
