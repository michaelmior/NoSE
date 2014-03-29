describe Planner do
  before(:each) do
    @entity = Entity.new('Tweet')
    @id_field = IDField.new('TweetId')
    @entity << @id_field
    @body_field = StringField.new('Body', 140)
    @entity << @body_field

    @time_field = IntegerField.new('Timestamp')
    @entity << @time_field

    @workload = Workload.new
    @workload.add_entity @entity
  end

  it 'can look up fields by key' do
    index = @entity.simple_index
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet'

    tree = planner.find_plans_for_query query
    expect(tree.first).to eq([IndexLookupStep.new(index)])
    expect(tree.count).to eq 1
  end

  it 'can perform an external sort if an index does not exist' do
    index = @entity.simple_index
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet ORDER BY Tweet.Timestamp'

    tree = planner.find_plans_for_query query
    steps = [IndexLookupStep.new(index), SortStep.new([@time_field])]
    expect(tree.first).to eq steps
    expect(tree.count).to eq 1
  end

  it 'raises an exception if there is no plan' do
    planner = Planner.new(@workload, [])
    query = Parser.parse 'SELECT Body FROM Tweet'
    expect { planner.find_plans_for_query query }.to \
        raise_error NoPlanException
  end

  it 'can find multiple plans' do
    index1 = Index.new([@time_field], [@body_field])
    index2 = Index.new([@id_field], [@time_field, @body_field])
    planner = Planner.new(@workload, [index1, index2])
    query = Parser.parse 'SELECT Body FROM Tweet ORDER BY Tweet.Timestamp'

    tree = planner.find_plans_for_query query
    expect(tree.to_a).to match_array [
      [IndexLookupStep.new(index1)],
      [IndexLookupStep.new(index2), SortStep.new([@time_field])]
    ]
  end

  it 'knows which fields are available at a given step' do
    index = Index.new([@id_field], [@body_field, @time_field])
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet'

    plan = planner.find_plans_for_query(query).first
    expect(plan.last.fields).to include(@id_field, @body_field, @time_field)
  end

  it 'can apply external filtering' do
    index = Index.new([@id_field], [@body_field, @time_field])
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet WHERE Tweet.Timestamp > 1'

    tree = planner.find_plans_for_query(query)
    expect(tree.count).to eq 1
    expect(tree.first.last).to eq FilterStep.new([], @time_field)
  end
end
