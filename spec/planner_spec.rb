require_relative '../lib/planner'
require_relative '../lib/model'
require_relative '../lib/workload'
require_relative '../lib/indexes'

describe Planner do
  before(:each) do
    @entity = Entity.new('Tweet')
    @id_field = IDField.new('TweetId')
    @entity << @id_field
    @entity << StringField.new('Body', 140)

    @time_field = IntegerField.new('Timestamp')
    @entity << @time_field

    @workload = Workload.new
    @workload.add_entity @entity
  end

  it 'can look up fields by key' do
    index = @entity.simple_index
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet'

    plan = planner.find_plan_for_query query, @workload
    expect(plan.first).to eq([IndexLookupStep.new(index)])
  end

  it 'can perform an external sort if an index does not exist' do
    index = @entity.simple_index
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet ORDER BY Tweet.Timestamp'

    plan = planner.find_plan_for_query query, @workload
    steps = [IndexLookupStep.new(index), SortStep.new([@time_field])]
    expect(plan.first).to eq steps
  end

  it 'raises an exception if there is no plan' do
    planner = Planner.new(@workload, [])
    query = Parser.parse 'SELECT Body FROM Tweet'
    expect { planner.find_plan_for_query query, @workload }.to \
        raise_error NoPlanException
  end
end
