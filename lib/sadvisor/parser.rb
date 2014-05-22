# Load our custom syntax node classes so the parser can use them
require_relative 'node_extensions'

module Sadvisor
  # A parser for a generic query language for NoSQL databases
  class Parser
    # Load the Treetop grammar from the 'cql_parser' file, and create a new
    # instance of that parser as a class variable so we don't have to re-create
    # it every time we need to parse a string
    Treetop.load(File.expand_path(File.join(File.dirname(__FILE__),
                                            'cql_parser.treetop')))
    @parser = CQLParser.new

    # Parse an input string and return an AST
    def self.parse(data)
      # Pass the data over to the parser instance
      tree = @parser.parse(data)

      # If the AST is nil then there was an error during parsing
      # we need to report a simple error message to help the user
      fail Exception, "Parse error at offset: #{@parser.index}" if tree.nil?

      # Remove all syntax nodes that aren't one of our custom
      # classes. If we don't do this we will end up with a *lot*
      # of essentially useless nodes
      clean_tree(tree)

      tree
    end

    # Remove unnecessary nodes
    def self.clean_tree(root_node)
      return if root_node.elements.nil?
      root_node.elements.delete_if do |node|
        node.class.name == 'Treetop::Runtime::SyntaxNode'
      end

      root_node.elements.each { |node| clean_tree(node) }
    end
    private_class_method :clean_tree
  end
end
