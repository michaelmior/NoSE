require_relative '../lib/planner'
require_relative '../lib/model'
require_relative '../lib/workload'
require_relative '../lib/indexes'


describe Planner do
  before (:each) do
    @entity = Entity.new('Tweet')
    @id_field = IDField.new('TweetId')
    @entity << @id_field
    @entity << StringField.new('Body', 140)

    @workload = Workload.new
    @workload.add_entity @entity
  end

  it 'can look up fields by key' do
    index = @entity.simple_index
    planner = Planner.new(@workload, [index])
    query = Parser.parse 'SELECT Body FROM Tweet'

    plan = planner.find_plan_for_query query, @workload
    expect(plan).to eq([IndexLookupStep.new(index)])
  end
end
