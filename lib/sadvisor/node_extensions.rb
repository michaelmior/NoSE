require 'treetop'

# Elements of a query parse tree
module CQL
  # Abstract class used for nodes in the query parse tree
  class CQLNode < Treetop::Runtime::SyntaxNode
    # Two nodes in the parse tree are equal if they have the same value
    def ==(other)
      if self.respond_to?(:value) && other.respond_to?(:value)
        value == other.value
      else
        super
      end
    end

    def inspect
      return value.to_s if self.respond_to? :value
      super()
    end
  end

  # A parsed query
  class Statement < CQLNode
    # All fields projected by this query
    def fields
      fields = elements.find do |n|
        [CQL::Identifier, CQL::IdentifierList].include? n.class
      end
      fields.class == CQL::Identifier ? [fields] : fields.elements
    end

    # Get the longest path through entities traversed in the query
    def longest_entity_path
      if where.length > 0
        fields = where.map { |condition| condition.field.value }
        fields += order_by.map(&:value)
        fields.max_by(&:count)[0..-2]  # last item is a field name
      else
        [from.value]
      end
    end

    # All conditions in the where clause of the query
    def where
      where = elements.find { |n| n.class == CQL::WhereClause }
      return [] if where.nil? || where.elements.length == 0

      conditions = []
      flatten_conditions = lambda do |node|
        if node.class.name == 'CQL::Condition'
          conditions.push node
        else
          node.elements.each(&flatten_conditions)
        end
      end
      flatten_conditions.call where

      conditions
    end

    # All fields with equality predicates in the where clause
    def eq_fields
      where.select { |condition| !condition.range? }
    end

    # The range predicate (if it exists) for this query
    def range_field
      where.find { |condition| condition.range? }
    end

    # The integer limit for the query, or +nil+ if no limit is given
    def limit
      limit = elements.find { |n| n.class == CQL::LimitClause }
      limit ? limit.value : nil
    end

    # The fields used in the order by clause for the query
    def order_by
      order_by = elements.find { |n| n.class == CQL::OrderByClause }
      order_by ? order_by.value : []
    end

    # The entity this query selects from
    def from
      elements.find { |n| [CQL::Entity].include? n.class }
    end
  end

  # A literal integer used in where clauses
  class IntegerLiteral < CQLNode
    # The integer value of the literal
    def value
      text_value.to_i
    end
  end

  # A literal float used in where clauses
  class FloatLiteral < CQLNode
    # The float value of the literal
    def value
      text_value.to_f
    end
  end

  # A literal string used in where clauses
  class StringLiteral < CQLNode
    # The string value of the literal with quotes removed
    def value
      text_value[1..-2]
    end
  end

  # A simple alphabetic identifier used in queries
  class Identifier < CQLNode
    # The string value of the identifier
    def value
      if parent.class == Statement || parent.class == IdentifierList
        statement = parent
        statement = statement.parent while statement.class != Statement
        entity = statement.elements.find \
            { |child| child.class == Entity }.value
        [entity, text_value.to_s]
      else
        text_value.to_s
      end
    end
  end

  # An entity name
  class Entity < Identifier
    def value
      text_value.to_s
    end
  end

  # A field in a query
  class Field < CQLNode
    # A list of identifiers comprising the field name
    def value
      elements.map do |n|
        n.class == CQL::Field ? n.elements.map { |m| m.value } : n.value
      end.flatten
    end
  end

  # The limit clause of a query
  class LimitClause < CQLNode
    # The integer value of the limit
    def value
      elements[0].text_value.to_i
    end
  end

  # A list of fields used for ordering clauses
  class FieldList < CQLNode
    # A list of names of each field
    def value
      elements.map { |n| n.value }
    end
  end

  # The ordering clause of a query
  class OrderByClause < CQLNode
    # The list fields being ordered on
    def value
      fields = elements[0]
      fields.class == CQL::Field ? [fields.value] : fields.value
    end
  end

  # A list of fields a query projects
  class IdentifierList < CQLNode
    # An array of fields
    def value
      elements.map { |n| n.value }
    end
  end

  # A where clause in a query
  class WhereClause < CQLNode
  end

  class Expression < CQLNode
  end

  # Represents a single predicate in a where clause
  class Condition < CQLNode
    # The field being compared
    def field
      elements[0]
    end

    # The value the field is being compared to
    def value
      elements[-1].value
    end

    # The operator this condition applies to
    def logical_operator
      elements.find { |n| n.class == CQL::Operator }
    end

    # Check if this is a range predicate
    def range?
      [:>, :>=, :<, :<=].include?(logical_operator.value)
    end
  end

  # An operator used for predicates in a where clause
  class Operator < CQLNode
    # A symbol representing the operator
    def value
      text_value.to_sym
    end
  end
end
