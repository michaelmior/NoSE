require 'hashids'
require 'representable'
require 'representable/json'

module Sadvisor
  # Represnts a field just by the entity and name
  class FieldRepresenter < Representable::Decorator
    include Representable::JSON

    property :entity, exec_context: :decorator
    property :name

    def entity
      represented.parent.name
    end
  end

  # Represents a simple key for an index
  class IndexRepresenter < Representable::Decorator
    include Representable::JSON

    property :key, exec_context: :decorator
    def key
      Hashids.new.encrypt(Zlib.crc32 represented.to_s)
    end
  end

  # Represents index data along with the key
  class FullIndexRepresenter < IndexRepresenter
    collection :hash_fields, decorator: FieldRepresenter
    collection :order_fields, decorator: FieldRepresenter
    collection :extra, decorator: FieldRepresenter

    property :path, exec_context: :decorator
    def path
      represented.path.map(&:name)
    end
  end

  # Represents all data of a field
  class EntityFieldRepresenter < Representable::Decorator
    include Representable::JSON

    property :name
    property :size
    property :cardinality
    property :subtype_name, as: :type
  end

  # Represent the whole entity and its fields
  class EntityRepresenter < Representable::Decorator
    include Representable::JSON

    property :name
    collection :fields, decorator: EntityFieldRepresenter,
                        exec_context: :decorator
    property :count

    def fields
      represented.fields.values
    end
  end

  # Conversion of a statement is just the text
  class StatementRepresenter < Representable::Decorator
    include Representable::JSON

    def to_hash(*)
      represented.query
    end
  end

  # Base representation for query plan steps
  class PlanStepRepresenter < Representable::Decorator
    include Representable::JSON

    property :subtype_name, as: :type
    property :cost
  end

  # Represent the index for index lookup plan steps
  class IndexLookupStepRepresenter < PlanStepRepresenter
    property :index, decorator: IndexRepresenter
  end

  # Represent the filtered fields in filter plan steps
  class FilterStepRepresenter < PlanStepRepresenter
    collection :eq, decorator: FieldRepresenter
    property :range, decorator: FieldRepresenter
  end

  # Represent the sorted fields in filter plan steps
  class SortStepRepresenter < PlanStepRepresenter
    collection :sort_fields, decorator: FieldRepresenter
  end

  # Represent a query plan as a sequence of steps
  class QueryPlanRepresenter < Representable::Decorator
    include Representable::JSON

    property :query, decorator: StatementRepresenter
    property :cost
    collection :each, as: :steps, decorator: (lambda do |step, *|
      {
        index_lookup: IndexLookupStepRepresenter,
        filter: FilterStepRepresenter,
        sort: SortStepRepresenter
      }[step.class.subtype_name.to_sym] || PlanStepRepresenter
    end)
  end
end
