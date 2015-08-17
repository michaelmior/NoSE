module NoSE
  # Tracks the runtime of various functions and outputs a measurement
  class Timer
    # Start tracking function runtime
    def self.enable
      traced = {
        IndexEnumerator => [
          :indexes_for_workload
        ],
        Search::Search => [
          :query_costs,
          :update_costs,
          :search_overlap,
          :solve_gurobi
        ],
        Search::Problem => [
          :setup_model,
          :add_variables,
          :add_constraints,
          :define_objective,
          :solve
        ]
      }
      @@old_methods = Hash.new { |h, k| h[k] = {} }

      # Redefine each method to capture timing information on each call
      traced.each do |cls, methods|
        methods.each do |method|
          old_method = cls.instance_method(method)
          cls.send(:define_method, method) do |*args|
            $stderr.puts "#{cls}##{method}\tSTART"

            start = Time.now
            result = old_method.bind(self).(*args)
            elapsed = Time.now - start

            $stderr.puts "#{cls}##{method}\tEND\t#{elapsed}"

            result
          end

          # Save a copy of the old method for later
          @@old_methods[cls][method] = old_method
        end
      end
    end

    # Stop tracking function runtime
    def self.disable
      @@old_methods.each do |cls, methods|
        methods.each do |method, old_method|
          cls.send(:define_method, method, old_method)
        end
      end

      # Remove the saved method definitions
      @@old_methods.clear
    end
  end
end
