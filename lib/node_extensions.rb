module CQL
  class CQLNode < Treetop::Runtime::SyntaxNode
    def ==(other)
      if self.respond_to? :value and other.respond_to? :value
        self.value == other.value
      else
        super
      end
    end
  end

  class Statement < CQLNode
    def fields
      fields = self.elements.detect {
          |n| ["CQL::Identifier", "CQL::IdentifierList"].include? n.class.name }
      fields.class.name == "CQL::Identifier" ? [fields] : fields.elements
    end

    def where
      where = self.elements.detect { |n| n.class.name == "CQL::WhereClause" }
      return [] if where.nil? or where.elements.length == 0
      where.elements.first.class.name == "CQL::Expression" ?
          where.elements.first.elements : where.elements
    end

    def eq_fields
      self.where.select { |condition| not condition.is_range? }
    end

    def range_field
      self.where.detect { |condition| condition.is_range? }
    end

    def limit
      limit = self.elements.detect { |n| n.class.name == "CQL::LimitClause" }
      limit ? limit.value : nil
    end

    def order_by
      order_by = self.elements.detect { |n| n.class.name == "CQL::OrderByClause" }
      order_by ? order_by.value : []
    end

    def from
      self.elements.detect { |n| ["CQL::Table"].include? n.class.name }
    end
  end

  class IntegerLiteral < CQLNode
    def value
      self.text_value.to_i
    end
  end

  class FloatLiteral < CQLNode
    def value
      self.text_value.to_f
    end
  end

  class StringLiteral < CQLNode
    def value
      self.text_value[1..-2]
    end
  end

  class Identifier < CQLNode
    def value
      self.text_value.to_s
    end
  end

  class Table < Identifier
  end

  class Field < CQLNode
    def value
      self.elements.map { |n|
          n.class.name == "CQL::Field" ?
              n.elements.map { |m| m.value } : n.value
      }.flatten
    end
  end

  class LimitClause < CQLNode
    def value
      self.elements[0].text_value.to_i
    end
  end

  class FieldList < CQLNode
    def value
      self.elements.map{ |n| n.value }
    end
  end

  class OrderByClause < CQLNode
    def value
      fields = self.elements[0]
      fields.class.name == "CQL::Field" ? [fields.value] : fields.value
    end
  end

  class IdentifierList < CQLNode
    def value
      self.elements.map{ |n| n.value }
    end
  end

  class WhereClause < CQLNode
  end

  class Expression < CQLNode
  end

  class Condition < CQLNode
    def field
      self.elements[0]
    end

    def value
      self.elements[-1].value
    end

    def logical_operator
      self.elements.detect { |n| n.class.name == "CQL::Operator" }
    end

    def is_range?
      [:>, :>=, :<, :<=].include?(self.logical_operator.value)
    end
  end

  class Operator < CQLNode
    def value
      self.text_value.to_sym
    end
  end
end
