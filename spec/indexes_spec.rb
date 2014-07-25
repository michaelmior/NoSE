module Sadvisor
  describe Index do
    let(:simple_query)   { Parser.parse('SELECT Id FROM Foo') }
    let(:equality_query) { Parser.parse('SELECT Id FROM Foo WHERE Foo.Id=3') }
    let(:range_query)    do Parser.parse('SELECT Id FROM Foo WHERE ' \
                                         'Foo.Id > 3')
    end
    let(:combo_query)    do Parser.parse('SELECT Id FROM Foo WHERE ' \
                                         'Foo.Id > 3 AND Foo.Bar = 1')
    end
    let(:order_query)    do Parser.parse('SELECT Id FROM Foo ' \
                                                  'ORDER BY Foo.Id')
    end
    let(:foreign_query)  do Parser.parse('SELECT Id FROM Foo WHERE ' \
                                         'Foo.Corge.Quux = 3')
    end

    before(:each) do
      @entity = Entity.new('Foo') * 100
      @id_field = IDField.new('Id')
      @field = IntegerField.new('Bar')
      @entity << @id_field
      @entity << @field

      @other_entity = Entity.new('Baz')
      @other_field = IntegerField.new('Quux')
      @other_entity << @other_field

      @foreign_key = ForeignKey.new('Corge', @other_entity)
      @entity << @foreign_key

      @workload = Workload.new
      @workload.add_query simple_query
      @workload.add_query equality_query
      @workload.add_query range_query
      @workload.add_query range_query
      @workload.add_query combo_query
      @workload.add_query order_query
      @workload.add_entity @entity
      @workload.add_entity @other_entity
    end

    it 'has zero size when empty' do
      expect(Index.new([], [], [], []).hash_fields).to be_empty
      expect(Index.new([], [], [], []).order_fields).to be_empty
      expect(Index.new([], [], [], []).entry_size).to eq 0
      expect(Index.new([], [], [], []).size).to eq 0
    end

    it 'contains fields' do
      index = Index.new([@id_field], [], [], [])
      expect(index.contains_field? @id_field).to be true
    end

    it 'can store additional fields' do
      index = Index.new([], [], [@id_field], [])
      expect(index.contains_field? @id_field).to be true
    end

    it 'can calculate its size' do
      index = Index.new([@id_field], [], [], [])
      @id_field *= 10
      expect(index.entry_size).to eq(@id_field.size)
      expect(index.size).to eq(@id_field.size * 10)
    end

    context 'when materializing views' do
      it 'supports simple lookups' do
        index = simple_query.materialize_view(@workload)
        expect(index.extra).to eq([@id_field])
      end

      it 'supports equality predicates' do
        index = equality_query.materialize_view(@workload)
        expect(index.hash_fields).to eq([@id_field])
      end

      it 'support range queries' do
        index = range_query.materialize_view(@workload)
        expect(index.order_fields).to eq([@id_field])
      end

      it 'supports multiple predicates' do
        index = combo_query.materialize_view(@workload)
        expect(index.hash_fields).to eq([@field])
        expect(index.order_fields).to eq([@id_field])
      end

      it 'supports order by' do
        index = order_query.materialize_view(@workload)
        expect(index.order_fields).to eq([@id_field])
      end
    end

    it 'can tell if it maps identities for a field' do
      index = Index.new([@id_field], [], [], [])
      expect(index.identity_for? @entity).to be true
    end

    it 'can be created to map entity fields by id' do
      index = @entity.simple_index
      expect(index.hash_fields).to eq([@id_field])
      expect(index.order_fields).to eq([])
      expect(index.extra).to eq([@field, @foreign_key])
    end
  end
end
