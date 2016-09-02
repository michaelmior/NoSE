# frozen_string_literal: true

module NoSE
  # Tracks the runtime of various functions and outputs a measurement
  class Timer
    # Start tracking function runtime
    # @return [void]
    def self.enable
      traced = {
        IndexEnumerator => [
          :indexes_for_workload,
          :support_indexes,
          :combine_indexes
        ],
        Search::Search => [
          :query_costs,
          :update_costs,
          :search_overlap,
          :solve_mipper
        ],
        Search::Problem => [
          :setup_model,
          :add_variables,
          :add_constraints,
          :define_objective,
          :total_cost,
          :add_update_costs,
          :total_size,
          :total_indexes,
          :solve
        ],
        MIPPeR::CbcModel => [
          :add_constraints,
          :add_variables,
          :update,
          :optimize
        ]
      }
      @old_methods = Hash.new { |h, k| h[k] = {} }

      # Redefine each method to capture timing information on each call
      traced.each do |cls, methods|
        methods.each do |method|
          old_method = cls.instance_method(method)
          cls.send(:define_method, method) do |*args|
            $stderr.puts "#{cls}##{method}\tSTART"

            start = Time.now.utc
            result = old_method.bind(self).call(*args)
            elapsed = Time.now.utc - start

            # Allow a block to be called with the timing results
            yield cls, method, elapsed if block_given?

            $stderr.puts "#{cls}##{method}\tEND\t#{elapsed}"

            result
          end

          # Save a copy of the old method for later
          @old_methods[cls][method] = old_method
        end
      end
    end

    # Stop tracking function runtime
    # @return [void]
    def self.disable
      @old_methods.each do |cls, methods|
        methods.each do |method, old_method|
          cls.send(:define_method, method, old_method)
        end
      end

      # Remove the saved method definitions
      @old_methods.clear
    end
  end
end
