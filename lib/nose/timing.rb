module NoSE
  # Tracks the runtime of various functions and outputs a measurement
  class Timer
    # Start tracking function runtime
    def self.enable
      @@timers = {}

      traced = {
        IndexEnumerator => [
          :indexes_for_workload
        ],
        Search::Search => [
          :costs,
          :search_overlap,
          :solve_gurobi
        ],
        Search::Problem => [
          :add_constraints,
          :define_objective
        ]
      }
      traced.default = []

      Kernel.set_trace_func proc { |event, _file, _line, \
                                    id, _binding, classname|
        next unless classname.respond_to? :hash
        if traced[classname].member?(id)
          if event == 'call'
            $stderr.puts "#{classname}.#{id}\tSTART"
            @@timers[[classname, id]] = Time.now
          elsif event == 'return'
            elapsed = Time.now - @@timers[[classname, id]]
            $stderr.puts "#{classname}.#{id}\tEND\t#{elapsed}"
          end
        end
      }
    end

    # Stop tracking function runtime
    def self.disable
      Kernel.set_trace_func nil
    end
  end
end
