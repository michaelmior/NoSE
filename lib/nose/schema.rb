module NoSE
  # Simple DSL for constructing indexes
  class Schema
    attr_reader :workload, :indexes

    def initialize(&block)
      @indexes = {}
      instance_eval(&block) if block_given?
    end

    # rubocop:disable MethodName

    # Set the workload to be used by the schema
    def Workload(name)
      filename = File.expand_path "../../../workloads/#{name}.rb", __FILE__
      contents = File.read(filename)
      @workload = binding.eval contents, filename

      NoSE::DSL.mixin_fields @workload.model.entities, IndexDSL
    end

    # Wrap commands for defining index attributes
    def Index(key, &block)
      # Apply the DSL
      dsl = IndexDSL.new(self)
      dsl.instance_eval(&block) if block_given?
      index = Index.new dsl.hash_fields, dsl.order_fields, dsl.extra,
                        dsl.path_keys, key
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
    def Hash(*fields)
      @hash_fields += fields.flatten
    end

    # Define a list of ordered fields
    def Ordered(*fields)
      @order_fields += fields.flatten
    end

    # Define a list of extra fields
    def Extra(*fields)
      @extra += fields.flatten
    end

    # Define the keys for the index path
    def Path(*keys)
      @path_keys += keys
    end

    # rubocop:enable MethodName
  end
end
