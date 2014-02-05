module CQL
  class CQLNode < Treetop::Runtime::SyntaxNode
    def ==(other)
      if self.respond_to? :value and other.respond_to? :value
        return self.value == other.value
      else
        return super
      end
    end
  end

  class Statement < CQLNode
    def fields
      fields = self.elements.detect {
          |n| ["CQL::Identifier", "CQL::IdentifierList"].include? n.class.name }
      return fields.class.name == "CQL::Identifier" ? [fields] : fields.elements
    end

    def where
      where = self.elements.detect { |n| n.class.name == "CQL::WhereClause" }
      return [] if where.nil? or where.elements.length == 0
      where.elements.first.class.name == "CQL::Expression" ?
          where.elements.first.elements : where.elements
    end

    def limit
      limit = self.elements.detect { |n| n.class.name == "CQL::LimitClause" }
      return limit ? limit.value : nil
    end

    def from
      return self.elements.detect {
          |n| ["CQL::Table"].include? n.class.name }
    end
  end

  class IntegerLiteral < CQLNode
    def value
      return self.text_value.to_i
    end
  end

  class FloatLiteral < CQLNode
    def value
      return self.text_value.to_f
    end
  end

  class StringLiteral < CQLNode
    def value
      return self.text_value[1..-2]
    end
  end

  class Identifier < CQLNode
    def value
      return self.text_value.to_s
    end
  end

  class Table < Identifier
  end

  class Field < CQLNode
    def entity
      return self.elements[0].value
    end

    def attribute
      return self.elements[1].value
    end

    def value
      return self.text_value
    end
  end

  class LimitClause < CQLNode
    def value
      return self.elements[0].text_value.to_i
    end
  end

  class IdentifierList < CQLNode
    def value
      return self.elements.map{ |n| n.value }.sort
    end
  end

  class WhereClause < CQLNode
  end

  class Expression < CQLNode
  end

  class Condition < CQLNode
    def field
      return self.elements[0]
    end

    def value
      return self.elements[-1].value
    end

    def logical_operator
      return self.elements.detect { |n| n.class.name == "CQL::Operator" }
    end
  end

  class Operator < CQLNode
    def value
      return self.text_value.to_sym
    end
  end
end
