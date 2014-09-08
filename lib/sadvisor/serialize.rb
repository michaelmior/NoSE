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

  # Represents index data along with a simple key
  class IndexRepresenter < Representable::Decorator
    include Representable::JSON

    collection :hash_fields, decorator: FieldRepresenter
    collection :order_fields, decorator: FieldRepresenter
    collection :extra, decorator: FieldRepresenter

    property :path, exec_context: :decorator
    def path
      represented.path.map(&:name)
    end

    property :key, exec_context: :decorator
    def key
      Hashids.new.encrypt(Zlib.crc32 represented.to_s)
    end
  end

  # Represents all data of a field
  class EntityFieldRepresenter < Representable::Decorator
    include Representable::JSON

    property :name
    property :size
    property :cardinality

    property :type, exec_context: :decorator
    def type
      represented.class.subtype_name
    end
  end

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

    def to_hash
      represented.query
    end
  end
end
