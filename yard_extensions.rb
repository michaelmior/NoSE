# frozen_string_literal: true

# Handler to add methods for Parslet rules
class ParsletHandler < YARD::Handlers::Ruby::Base
  handles method_call(:rule)
  namespace_only

  # Add a method for each Parlset rule
  def process
    name = statement.parameters.first.jump(:tstring_content, :ident).source
    object = YARD::CodeObjects::MethodObject.new namespace, name
    register(object)
    parse_block(statement.last.last, owner: object)
  end
end
