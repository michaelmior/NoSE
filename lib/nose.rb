# Namespace module for the whole project
module NoSE
end

require_relative 'nose/util'

require_relative 'nose/backend'
require_relative 'nose/enumerator'
require_relative 'nose/indexes'
require_relative 'nose/loaders'
require_relative 'nose/model'
require_relative 'nose/parser'
require_relative 'nose/planner'
require_relative 'nose/proxy'
require_relative 'nose/random'
require_relative 'nose/search'
require_relative 'nose/timing'
require_relative 'nose/workload'

require_relative 'nose/serialize'

# :nocov:
if ENV['NOSE_LOG']
  require 'logging'

  logger = Logging.logger['nose']
  logger.level = ENV['NOSE_LOG'].downcase.to_sym
  logger.add_appenders Logging.appenders.stderr
  logger = nil # rubocop:disable Lint/UselessAssignment
end
# :nocov: