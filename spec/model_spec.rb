module NoSE
  describe Entity do
    subject(:entity) { Entity.new('Foo') }

    it 'can store fields' do
      entity << Fields::IntegerField.new('Bar')
      entity << Fields::IntegerField.new('Baz')

      expect(entity.fields.keys).to match_array %w(Bar Baz)
    end

    it 'can have foreign keys' do
      other = entity * 100
      field = Fields::ForeignKeyField.new('other', other)
      entity << field

      expect(field.entity).to be(other)
      expect(field.class.subtype_name).to eq('foreign_key')
      expect(field.relationship).to eq(:one)
      expect(field.cardinality).to eq(100)
    end

    it 'can tell fields when they are added' do
      field = Fields::IntegerField.new('Bar')

      expect(field.parent).to be_nil

      entity << field

      expect(field.parent).to be(entity)
    end

    it 'can create entities using a DSL' do
      entity = Entity.new 'Foo' do
        ID 'Bar'
        Integer 'Baz'
        String 'Quux', 20
        etc
      end

      expect(entity.fields).to have(4).items
      expect(entity.fields['Quux'].size).to eq 20
      expect(entity['**'].class).to be Fields::HashField
    end

    it 'raises an exception for nonexistent fields' do
      expect { entity['Bar'] }.to raise_error FieldNotFound
    end

    it 'can generate random entities' do
      entity << Fields::IntegerField.new('Bar')
      expect(entity.random_entity).to be_a Hash
      expect(entity.random_entity.keys).to match_array ['Foo_Bar']
    end
  end

  describe Fields::Field do
    subject(:field) do
      Fields::IDField.new 'Bar'
    end

    it 'has an ID based on the entity and name' do
      Entity.new('Foo') << field
      expect(field.id).to eq 'Foo_Bar'
    end

    it 'can have its cardinality updated by multiplication' do
      expect((field * 5).cardinality).to eq 5
    end
  end

  describe Fields::IntegerField do
    it 'can convert string literals' do
      expect(Fields::IntegerField.value_from_string '42').to eq 42
    end

    it 'can produce random integers' do
      field = Fields::IntegerField.new 'Foo', count: 10
      expect(field.random_value).to be_a Integer
      expect(field.random_value).to be_between(0, field.cardinality)
    end
  end

  describe Fields::FloatField do
    it 'can convert string literals' do
      expect(Fields::FloatField.value_from_string '3.14159').to eq 3.14159
    end

    it 'can produce random floats' do
      field = Fields::FloatField.new 'Foo', count: 10
      expect(field.random_value).to be_a Float
      expect(field.random_value).to be_between(0, field.cardinality)
    end
  end

  describe Fields::StringField do
    it 'can convert string literals' do
      expect(Fields::StringField.value_from_string 'pudding').to eq 'pudding'
    end

    it 'can produce random strings' do
      field = Fields::StringField.new 'Foo', 10
      expect(field.random_value).to be_a String
      expect(field.random_value).to have(10).characters
    end
  end

  describe Fields::DateField do
    it 'can convert string literals' do
      date = Fields::DateField.value_from_string '2001-02-03T04:05:06+07:00'
      expect(date).to eq DateTime.new(2001, 2, 3, 4, 5, 6, '+7').to_time
    end

    it 'can produce random dates' do
      field = Fields::DateField.new 'Foo'
      expect(field.random_value).to be_a Time
    end
  end

  describe Fields::BooleanField do
    it 'can convert boolean strings' do
      expect(Fields::BooleanField.value_from_string 'false').to be_falsey
    end

    it 'can convert integers in strings' do
      expect(Fields::BooleanField.value_from_string '1').to be_truthy
    end

    it 'can produce random booleans' do
      field = Fields::BooleanField.new 'Foo'
      expect(field.random_value).to satisfy { |v| [true, false].include? v }
    end
  end

  describe Model do
    let(:model) do
      Model.new do
        Entity 'Foo' do
          ID 'FooID'
        end

        Entity 'Bar' do
          ID 'BarID'
        end

        HasOne 'foo',    'bars',
               'Bar'  => 'Foo'
      end
    end

    it 'can create a to-one relationship' do
      bar_key = model.entities['Bar']['foo']
      expect(bar_key.relationship).to eq(:one)
    end

    it 'can create a to-many relationship' do
      foo_key = model.entities['Foo']['bars']
      expect(foo_key.relationship).to eq(:many)
    end
  end
end
