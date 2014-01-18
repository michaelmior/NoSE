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
          |n| ["CQL::Field", "CQL::FieldList"].include? n.class.name }
      return fields.class.name == "CQL::Field" ? [fields] : fields.elements
    end

    def where
      where =  self.elements.detect { |n| n.class.name == "CQL::WhereClause" }
      return where.class.name == "CQL::Condition" ?
          [where] : where.elements[0].elements
    end

    def limit
      limit = self.elements.detect { |n| n.class.name == "CQL::LimitClause" }
      return limit ? limit.value : nil
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
      return self.text_value
    end
  end

  class Identifier < CQLNode
    def value
      return self.text_value.to_s
    end
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

  class FieldList < CQLNode
    def value
      return self.elements.map{ |n| n.value }.sort
    end
  end

  class WhereClause < CQLNode
  end

  class Expression < CQLNode
  end

  class Condition < CQLNode
  end

  class Operator < CQLNode
    def value
      return self.text_value.to_sym
    end
  end
end
