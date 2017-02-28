# frozen_string_literal: true

module NoSE
  # Simple DSL for constructing indexes
  class Schema
    attr_reader :model, :indexes

    def initialize(&block)
      @indexes = {}
      instance_eval(&block) if block_given?
    end

    # Find the schema with the given name
    def self.load(name)
      filename = File.expand_path "../../../schemas/#{name}.rb", __FILE__
      contents = File.read(filename)
      binding.eval contents, filename
    end

    # rubocop:disable MethodName

    # Set the model to be used by the schema
    # @return [void]
    def Model(name)
      @model = Model.load name
      NoSE::DSL.mixin_fields @model.entities, IndexDSL
    end

    # Add a simple index for an entity
    # @return [void]
    def SimpleIndex(entity)
      @indexes[entity] = @model[entity].simple_index
    end

    # Wrap commands for defining index attributes
    # @return [void]
    def Index(key, &block)
      # Apply the DSL
      dsl = IndexDSL.new(self)
      dsl.instance_eval(&block) if block_given?
      index = Index.new dsl.hash_fields, dsl.order_fields, dsl.extra,
                        QueryGraph::Graph.from_path(dsl.path_keys),
                        saved_key: key
      @indexes[index.key] = index
    end

    # rubocop:enable MethodName
  end

  # DSL for index creation within a schema
  class IndexDSL
    attr_reader :hash_fields, :order_fields, :extra, :path_keys

    def initialize(schema)
      @schema = schema
      @hash_fields = []
      @order_fields = []
      @extra = []
      @path_keys = []
    end

    # rubocop:disable MethodName

    # Define a list of hash fields
    # @return [void]
    def Hash(*fields)
      @hash_fields += fields.flatten
    end

    # Define a list of ordered fields
    # @return [void]
    def Ordered(*fields)
      @order_fields += fields.flatten
    end

    # Define a list of extra fields
    # @return [void]
    def Extra(*fields)
      @extra += fields.flatten
    end

    # Define the keys for the index path
    # @return [void]
    def Path(*keys)
      @path_keys += keys
    end

    # rubocop:enable MethodName
  end
end
