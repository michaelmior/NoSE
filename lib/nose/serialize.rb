require 'representable'
require 'representable/json'
require 'representable/yaml'

module NoSE
  # Serialization of workloads and statement execution plans
  module Serialize
    # Construct a field from a parsed hash
    class FieldBuilder
      include Uber::Callable

      def call(_object, _fragment, instance, **options)
        field_class = Fields::Field.subtype_class instance['type']

        # Extract the correct parameters and create a new field instance
        if field_class == Fields::StringField && !instance['size'].nil?
          field = field_class.new instance['name'], instance['size']
        elsif field_class.ancestors.include? Fields::ForeignKeyField
          field = field_class.new instance['name'],
                                  options[:entity_map][instance['entity']]
        else
          field = field_class.new instance['name']
        end

        field *= instance['cardinality'] if instance['cardinality']

        field
      end
    end

    # Represents a field just by the entity and name
    class FieldRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      property :name

      # The name of the parent entity
      def parent
        represented.parent.name
      end
      property :parent, exec_context: :decorator
    end

    # Reconstruct indexes with fields from an existing workload
    class IndexBuilder
      include Uber::Callable

      def call(object, _fragment, instance, **_options)
        # Extract the entities from the workload
        workload = object.workload
        model = workload.model

        # Pull the fields from each entity
        f = lambda do |fields|
          instance[fields].map { |dict| model[dict['parent']][dict['name']] }
        end

        Index.new f.call('hash_fields'), f.call('order_fields'),
                  f.call('extra'), f.call('path'), instance['key']
      end
    end

    # Represents a simple key for an index
    class IndexRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      property :key
    end

    # Represents index data along with the key
    class FullIndexRepresenter < IndexRepresenter
      collection :hash_fields, decorator: FieldRepresenter
      collection :order_fields, decorator: FieldRepresenter
      collection :extra, decorator: FieldRepresenter
      collection :path, decorator: FieldRepresenter

      property :size
    end

    # Represents all data of a field
    class EntityFieldRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      collection_representer class: Object, deserialize: FieldBuilder.new

      property :name
      property :size
      property :cardinality
      property :subtype_name, as: :type

      # The entity name for foreign keys
      def entity
        represented.entity.name \
          if represented.is_a? Fields::ForeignKeyField
      end
      property :entity, exec_context: :decorator

      # The cardinality of the relationship
      def relationship
        represented.relationship \
          if represented.is_a? Fields::ForeignKeyField
      end

      # Convert the relationship type to a symbol
      def relationship=(relationship)
        represented.relationship = relationship.to_sym
      end

      property :relationship, exec_context: :decorator

      # The reverse
      def reverse
        represented.reverse.name \
          if represented.is_a? Fields::ForeignKeyField
      end

      property :reverse, exec_context: :decorator
    end

    # Reconstruct the fields of an entity
    class EntityBuilder
      include Uber::Callable

      def call(_object, _fragment, instance, **options)
        # Pull the field from the map of all entities
        entity = options[:entity_map][instance['name']]

        # Add all fields from the entity
        fields = EntityFieldRepresenter.represent([])
        fields = fields.from_hash instance['fields'],
                                  entity_map: options[:entity_map]
        fields.each { |field| entity.send(:<<, field, freeze: false) }

        entity
      end
    end

    # Represent the whole entity and its fields
    class EntityRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      collection_representer class: Object, deserialize: EntityBuilder.new

      property :name
      collection :fields, decorator: EntityFieldRepresenter,
                          exec_context: :decorator
      property :count

      # A simple array of the fields within the entity
      def fields
        represented.fields.values + represented.foreign_keys.values
      end
    end

    # Conversion of a statement is just the text
    class StatementRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      # Represent as the text of the statement
      def to_hash(*)
        represented.text
      end
    end

    # Base representation for query plan steps
    class PlanStepRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      property :subtype_name, as: :type

      # The estimated cardinality at this step in the plan
      def cardinality
        represented.instance_variable_get(:@state).cardinality
      end
      property :cardinality, exec_context: :decorator
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

    # Represent the limit for limit plan steps
    class LimitStepRepresenter < PlanStepRepresenter
      property :limit
    end

    # Represent a query plan as a sequence of steps
    class QueryPlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      property :query, decorator: StatementRepresenter
      property :cost
      collection :each, as: :steps, decorator: (lambda do |step, *|
        {
          index_lookup: IndexLookupStepRepresenter,
          filter: FilterStepRepresenter,
          sort: SortStepRepresenter,
          limit: LimitStepRepresenter
        }[step.class.subtype_name.to_sym] || PlanStepRepresenter
      end)
    end

    # Represent entities and statements in a workload
    class WorkloadRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      collection :statements, decorator: StatementRepresenter

      # A simple array of the entities in the workload
      def entities
        represented.model.entities.values
      end
      collection :entities, decorator: EntityRepresenter,
                            exec_context: :decorator
    end

    # Construct a new workload from a parsed hash
    class WorkloadBuilder
      include Uber::Callable

      def call(_object, fragment, instance, **_options)
        workload = fragment.represented

        # Recreate all the entities
        entity_map = {}
        instance['entities'].each do |entity_hash|
          entity_map[entity_hash['name']] = Entity.new entity_hash['name']
        end

        # Populate the entities and add them to the workload
        entities = EntityRepresenter.represent([])
        entities = entities.from_hash instance['entities'],
                                      entity_map: entity_map
        entities.each { |entity| workload << entity }

        # Add all the reverse foreign keys
        instance['entities'].each do |entity|
          entity['fields'].each do |field_hash|
            if field_hash['type'] == 'foreign_key'
              field = entity_map[entity['name']] \
                .foreign_keys[field_hash['name']]
              field.reverse = field.entity.foreign_keys[field_hash['reverse']]
            end
            field.freeze
          end
        end

        # Add all statements to the workload
        instance['statements'].each do |statement|
          workload.add_statement statement
        end

        workload
      end
    end

    # Reconstruct the steps of a query plan
    class QueryPlanBuilder
      include Uber::Callable

      def call(object, _fragment, instance, **_options)
        workload = object.workload
        query = Query.new instance['query'], workload.model

        plan = Plans::QueryPlan.new query, object.cost_model
        state = Plans::QueryState.new query, workload
        parent = Plans::RootPlanStep.new state

        f = ->(field) { workload.model[field['parent']][field['name']] }

        # Loop over all steps in the plan and reconstruct them
        instance['steps'].each do |step_hash|
          step_class = Plans::PlanStep.subtype_class step_hash['type']
          if step_class == Plans::IndexLookupPlanStep
            index_key = step_hash['index']['key']
            step_index = object.indexes.find { |index| index.key == index_key }
            step = step_class.new step_index, state, parent.state
          elsif step_class == Plans::FilterPlanStep
            eq = step_hash['eq'].map(&f)
            range = f.call(step_hash['range']) if step_hash['range']
            step = step_class.new eq, range, parent.state
          elsif step_class == Plans::SortPlanStep
            sort_fields = step_hash['sort_fields'].map(&f)
            step = step_class.new sort_fields, parent.state
          elsif step_class == Plans::LimitPlanStep
            limit = step_hash['limit'].to_i
            step = step_class.new limit, parent.state
          end

          # Copy the correct cardinality
          # XXX This may not preserve all the necessary state
          state = step.state.dup
          state.instance_variable_set :@cardinality,
                                      step_hash['cardinality']
          step.state = state.freeze

          # Force setting of the parent step
          step.instance_variable_set :@parent, parent

          plan << step
          parent = step
        end

        plan
      end
    end

    # Represent results of a search operation
    class SearchResultRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML

      property :workload, decorator: WorkloadRepresenter,
                          class: Workload,
                          deserialize: WorkloadBuilder.new
      collection :indexes, decorator: FullIndexRepresenter,
                           class: Object,
                           deserialize: IndexBuilder.new
      collection :enumerated_indexes, decorator: FullIndexRepresenter,
                                      class: Object,
                                      deserialize: IndexBuilder.new

      # The backend cost model used to generate the schema
      def cost_model
        represented.cost_model.subtype_name
      end

      # Look up the cost model by name and attach to the results
      def cost_model=(cost_model)
        represented.cost_model = Cost::Cost.subtype_class cost_model
      end

      property :cost_model, exec_context: :decorator

      collection :plans, decorator: QueryPlanRepresenter,
                         class: Object,
                         deserialize: QueryPlanBuilder.new
      property :total_size
      property :total_cost

      # Include the revision of the code used to generate this output
      def revision
        `git rev-parse HEAD 2> /dev/null`.strip
      end

      # Set the revision string on the results object
      def revision=(revision)
        represented.revision = revision
      end

      property :revision, exec_context: :decorator

      # The time the results were generated
      def time
        Time.now.rfc2822
      end

      # Reconstruct the time object from the timestamp
      def time=(time)
        represented.time = Time.rfc2822 time
      end

      property :time, exec_context: :decorator

      # The full command used to generate the results
      def command
        "#{$PROGRAM_NAME} #{ARGV.join ' '}"
      end

      # Set the command string on the results object
      def command=(command)
        represented.command = command
      end

      property :command, exec_context: :decorator
    end
  end
end
