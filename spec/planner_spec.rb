describe Sadvisor::Planner do
  before(:each) do
    @entity = Sadvisor::Entity.new('Tweet')
    @id_field = Sadvisor::IDField.new('TweetId')
    @entity << @id_field
    @body_field = Sadvisor::StringField.new('Body', 140)
    @entity << @body_field

    @time_field = Sadvisor::IntegerField.new('Timestamp')
    @entity << @time_field

    @workload = Sadvisor::Workload.new
    @workload.add_entity @entity
  end

  it 'can look up fields by key' do
    index = @entity.simple_index
    planner = Sadvisor::Planner.new(@workload, [index])
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet'

    tree = planner.find_plans_for_query query
    expect(tree.first).to eq([Sadvisor::IndexLookupStep.new(index)])
    expect(tree).to have(1).item
    expect(tree.first.cost).to be > 0
  end

  it 'can perform an external sort if an index does not exist' do
    index = @entity.simple_index
    planner = Sadvisor::Planner.new(@workload, [index])
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet ORDER BY ' \
                                   'Tweet.Timestamp'

    tree = planner.find_plans_for_query query
    steps = [Sadvisor::IndexLookupStep.new(index),
             Sadvisor::SortStep.new([@time_field])]
    expect(tree.first).to eq steps
    expect(tree).to have(1).item
  end

  it 'raises an exception if there is no plan' do
    planner = Sadvisor::Planner.new(@workload, [])
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet'
    expect { planner.find_plans_for_query query }.to \
        raise_error Sadvisor::NoPlanException
  end

  it 'can find multiple plans' do
    index1 = Sadvisor::Index.new([@time_field], [@body_field])
    index2 = Sadvisor::Index.new([@id_field], [@time_field, @body_field])
    planner = Sadvisor::Planner.new(@workload, [index1, index2])
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet ORDER BY ' \
                                   'Tweet.Timestamp'

    tree = planner.find_plans_for_query query
    expect(tree.to_a).to match_array [
      [Sadvisor::IndexLookupStep.new(index1)],
      [Sadvisor::IndexLookupStep.new(index2),
       Sadvisor::SortStep.new([@time_field])]
    ]
  end

  it 'knows which fields are available at a given step' do
    index = Sadvisor::Index.new([@id_field], [@body_field, @time_field])
    planner = Sadvisor::Planner.new(@workload, [index])
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet'

    plan = planner.find_plans_for_query(query).first
    expect(plan.last.fields).to include(@id_field, @body_field, @time_field)
  end

  it 'can apply external filtering' do
    index = Sadvisor::Index.new([@id_field], [@body_field, @time_field])
    planner = Sadvisor::Planner.new(@workload, [index])
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet WHERE ' \
                                   'Tweet.Timestamp > 1'

    tree = planner.find_plans_for_query(query)
    expect(tree).to have(1).item
    expect(tree.first.last).to eq Sadvisor::FilterStep.new([], @time_field)
  end

  it 'can perform a separate lookup by ID' do
    query = Sadvisor::Parser.parse 'SELECT Body FROM Tweet WHERE ' \
                                   'Tweet.Timestamp = 1'
    time_index = Sadvisor::Index.new [@time_field], [@id_field]
    id_index = Sadvisor::Index.new [@id_field], [@body_field]

    planner = Sadvisor::Planner.new(@workload, [time_index, id_index])
    tree = planner.find_plans_for_query(query)

    expect(tree).to include [
      Sadvisor::IndexLookupStep.new(time_index),
      Sadvisor::IDLookupStep.new(id_index)
    ]
  end
end
