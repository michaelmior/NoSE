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
  end

  # A parsed query
  class Statement < CQLNode
    # All fields projected by this query
    def fields
      fields = elements.find do |n|
        ['CQL::Identifier', 'CQL::IdentifierList'].include? n.class.name
      end
      fields.class.name == 'CQL::Identifier' ? [fields] : fields.elements
    end

    # All conditions in the where clause of the query
    def where
      where = elements.find { |n| n.class.name == 'CQL::WhereClause' }
      return [] if where.nil? || where.elements.length == 0

      if where.elements.first.class.name == 'CQL::Expression'
        where.elements.first.elements
      else
        where.elements
      end
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
      limit = elements.find { |n| n.class.name == 'CQL::LimitClause' }
      limit ? limit.value : nil
    end

    # The fields used in the order by clause for the query
    def order_by
      order_by = elements.find { |n| n.class.name == 'CQL::OrderByClause' }
      order_by ? order_by.value : []
    end

    # The table this query selects from
    def from
      elements.find { |n| ['CQL::Table'].include? n.class.name }
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
      text_value.to_s
    end
  end

  # A table name
  class Table < Identifier
  end

  # A field in a query
  class Field < CQLNode
    # A list of identifiers comprising the field name
    def value
      elements.map do |n|
        n.class.name == 'CQL::Field' ? n.elements.map { |m| m.value } : n.value
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
      fields.class.name == 'CQL::Field' ? [fields.value] : fields.value
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
      elements.find { |n| n.class.name == 'CQL::Operator' }
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
