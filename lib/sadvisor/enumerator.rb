# Produces potential indices to be used in schemas
class IndexEnumerator
  # Produce all possible indices for a given set of fields
  def self.indexes_for_fields(fields)
    Enumerator.new do |enum|
      0.upto(fields.count) do |num_fields|
        fields.permutation(num_fields) do |index_fields|
          0.upto(fields.count - num_fields) do |num_extra|
            (fields - index_fields).combination(num_extra) do |extra|
              enum.yield Index.new index_fields, extra \
                  if index_fields.count > 0
            end
          end
        end
      end
    end
  end

  # Produce all possible indices for a given entity
  def self.indexes_for_entity(entity)
    indexes_for_fields entity.fields.values
  end

  # Produce all possible indices for a given query
  def self.indexes_for_query(query, workload)
    fields = query.fields.map { |field| workload.find_field field.value }
    fields += query.eq_fields.map \
        { |condition| workload.find_field condition.field.value }

    fields << (workload.find_field query.range_field.field.value) \
        unless query.range_field.nil?

    indexes_for_fields fields
  end

  def self.indexes_for_workload(workload)
    workload.queries.map do |query|
      indexes_for_query(query, workload).to_a
    end.inject([], &:+)
  end
end
