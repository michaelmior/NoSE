# frozen_string_literal: true

module NoSE
  module Loader
    # Load some random data (mostly useful for testing)
    class RandomLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        @logger = Logging.logger['nose::loader::randomloader']

        @workload = workload
        @backend = backend
      end

      # Load a generated set of indexes with data from MySQL
      # @return [void]
      def load(indexes, config, show_progress = false, limit = nil,
               skip_existing = true)
        limit = 1 if limit.nil?

        indexes.map!(&:to_id_graph).uniq! if @backend.by_id_graph
        indexes.uniq.each do |index|
          load_index index, config, show_progress, limit, skip_existing
        end
      end

      private

      # Load a single index into the backend
      # @return [void]
      def load_index(index, _config, show_progress, limit, skip_existing)
        # Skip this index if it's not empty
        if skip_existing && !@backend.index_empty?(index)
          @logger.info "Skipping index #{index.inspect}" if show_progress
          return
        end
        @logger.info index.inspect if show_progress

        chunk = Array.new(limit) do
          Hash[index.all_fields.map do |field|
            [field.id, field.random_value]
          end]
        end

        @backend.index_insert_chunk index, chunk
      end
    end
  end
end
