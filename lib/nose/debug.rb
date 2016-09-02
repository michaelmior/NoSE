# frozen_string_literal: true
# rubocop:disable Lint/HandleExceptions
begin
  require 'binding_of_caller'
  require 'pry'
rescue LoadError
  # Ignore in case we are not in development mode
end
# rubocop:enable Lint/HandleExceptions

module NoSE
  # Various helpful debugging snippets
  module Debug
    # Convenience method to break in IndexLookupStep when
    # a particular set of indexes is reach when planning
    # @return [void]
    def self.break_on_indexes(*index_keys)
      apply = binding.of_caller(1)
      parent = apply.eval 'parent'
      index = apply.eval 'index'
      current_keys = parent.parent_steps.indexes.map(&:key) << index.key

      # rubocop:disable Lint/Debugger
      binding.pry if current_keys == index_keys
      # rubocop:enable Lint/Debugger
    end

    # Export entities in a model as global
    # variales for easier access when debugging
    # @return [void]
    def self.export_model(model)
      model.entities.each do |name, entity|
        # rubocop:disable Lint/Eval
        eval("$#{name} = entity")
        # rubocop:enable Lint/Eval

        entity.fields.merge(entity.foreign_keys).each do |field_name, field|
          entity.define_singleton_method field_name.to_sym, -> { field }
        end
      end

      nil
    end
  end
end
