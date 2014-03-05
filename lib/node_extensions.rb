require 'treetop'

module CQL
  class CQLNode < Treetop::Runtime::SyntaxNode
    def ==(other)
      if self.respond_to?(:value) && other.respond_to?(:value)
        value == other.value
      else
        super
      end
    end
  end

  class Statement < CQLNode
    def fields
      fields = elements.find do |n|
        ['CQL::Identifier', 'CQL::IdentifierList'].include? n.class.name
      end
      fields.class.name == 'CQL::Identifier' ? [fields] : fields.elements
    end

    def where
      where = elements.find { |n| n.class.name == 'CQL::WhereClause' }
      return [] if where.nil? || where.elements.length == 0

      if where.elements.first.class.name == 'CQL::Expression'
        where.elements.first.elements
      else
        where.elements
      end
    end

    def eq_fields
      where.select { |condition| !condition.range? }
    end

    def range_field
      where.find { |condition| condition.range? }
    end

    def limit
      limit = elements.find { |n| n.class.name == 'CQL::LimitClause' }
      limit ? limit.value : nil
    end

    def order_by
      order_by = elements.find { |n| n.class.name == 'CQL::OrderByClause' }
      order_by ? order_by.value : []
    end

    def from
      elements.find { |n| ['CQL::Table'].include? n.class.name }
    end
  end

  class IntegerLiteral < CQLNode
    def value
      text_value.to_i
    end
  end

  class FloatLiteral < CQLNode
    def value
      text_value.to_f
    end
  end

  class StringLiteral < CQLNode
    def value
      text_value[1..-2]
    end
  end

  class Identifier < CQLNode
    def value
      text_value.to_s
    end
  end

  class Table < Identifier
  end

  class Field < CQLNode
    def value
      elements.map do |n|
        n.class.name == 'CQL::Field' ? n.elements.map { |m| m.value } : n.value
      end.flatten
    end
  end

  class LimitClause < CQLNode
    def value
      elements[0].text_value.to_i
    end
  end

  class FieldList < CQLNode
    def value
      elements.map { |n| n.value }
    end
  end

  class OrderByClause < CQLNode
    def value
      fields = elements[0]
      fields.class.name == 'CQL::Field' ? [fields.value] : fields.value
    end
  end

  class IdentifierList < CQLNode
    def value
      elements.map { |n| n.value }
    end
  end

  class WhereClause < CQLNode
  end

  class Expression < CQLNode
  end

  class Condition < CQLNode
    def field
      elements[0]
    end

    def value
      elements[-1].value
    end

    def logical_operator
      elements.find { |n| n.class.name == 'CQL::Operator' }
    end

    def range?
      [:>, :>=, :<, :<=].include?(logical_operator.value)
    end
  end

  class Operator < CQLNode
    def value
      text_value.to_sym
    end
  end
end
