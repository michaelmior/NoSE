describe 'Hotel example' do
  before(:each) do
    @w = w = Workload.new

    @w << Entity.new('POI') do
      ID 'POIID'
      String 'Name', 20
      String 'Description', 200
    end

    @w << Entity.new('Hotel') do
      ID 'HotelID'
      String 'Name', 20
      String 'Phone', 10
      String 'Address', 50
      String 'City', 20
      String 'Zip', 5
      ToManyKey 'POIs', w['POI']
    end

    @w << Entity.new('Amenity') do
      ID 'AmenityID'
      String 'Name', 20
    end

    @w << Entity.new('Room') do
      ID 'RoomID'
      ForeignKey 'HotelID', w['Hotel']
      String 'RoomNumber', 4
      Float 'Rate'
      ToManyKey 'Amenities', w['Amenity']
    end

    @w << Entity.new('Guest') do
      ID 'GuestID'
      String 'Name', 20
      String 'Email', 20
    end

    @w << Entity.new('Reservation') do
      ID 'ReservationID'
      ForeignKey 'GuestID', w['Guest']
      ForeignKey 'RoomID', w['Room']
      Date 'StartDate'
      Date 'EndDate'
    end

    @query = Parser.parse 'SELECT Name FROM POI WHERE ' \
                          'POI.Hotel.Room.Reservation.Guest.GuestID = 3'
    @w.add_query @query
  end

  it 'can look up entities via multiple foreign keys' do
    guest_id = @w['Guest']['GuestID']
    index = Index.new([guest_id], [@w['POI']['Name']])
    index.set_field_keys guest_id, \
                         [@w['Hotel']['Room'],
                          @w['Room']['Reservation'],
                          guest_id]
    planner = Planner.new @w, [index]
    tree = planner.find_plans_for_query @query
    expect(tree.count).to eq 1
    expect(tree.first).to match_array [IndexLookupStep.new(index)]
  end

  it 'uses the workload to find foreign key traversals' do
    fields = @w.find_field_keys %w{Hotel Room Reservation Guest GuestID}
    expect(fields).to eq \
        [[@w['Guest']['GuestID']],
         [@w['Reservation']['ReservationID']],
         [@w['Room']['RoomID']],
         [@w['Hotel']['HotelID']]]
  end

  it 'uses the workload to populate all relevant tables' do
    tables = QueryState.new(@query, @w).tables
    expect(tables).to include(
      @w['Guest'] => [],
      @w['POI'] => [[@w['Guest']['GuestID']],
                    [@w['Reservation']['ReservationID']],
                    [@w['Room']['RoomID']],
                    [@w['Hotel']['HotelID']]],
      @w['Reservation'] => [[@w['Guest']['GuestID']]],
      @w['Room'] => [[@w['Guest']['GuestID']],
                     [@w['Reservation']['ReservationID']]],
      @w['Hotel'] => [[@w['Guest']['GuestID']],
                      [@w['Reservation']['ReservationID']],
                      [@w['Room']['RoomID']]])
  end

  it 'can look up entities using multiple indices' do
    simple_indexes = @w.entities.values.map(&:simple_index)
    planner = Planner.new @w, simple_indexes
    tree = planner.find_plans_for_query @query
    expect(tree.count).to eq 1
    expect(tree.first).to match_array [
        IndexLookupStep.new(@w['Reservation'].simple_index),
        IndexLookupStep.new(@w['Room'].simple_index),
        IndexLookupStep.new(@w['Hotel'].simple_index),
        IndexLookupStep.new(@w['POI'].simple_index),
        FilterStep.new([@w['Guest']['GuestID']], nil)]
  end

  it 'can enumerate all simple indices' do
    @w.entities.values.each do |entity|
      simple_index = entity.simple_index
      indexes = IndexEnumerator.indexes_for_entity entity
      expect(indexes).to include simple_index
    end
  end

  it 'can enumerate a materialized view' do
    view = @query.materialize_view(@w)
    indexes = IndexEnumerator.indexes_for_workload @w
    expect(indexes).to include view
  end

  it 'can select from multiple plans' do
    indexes = @w.entities.values.map(&:simple_index)
    view = @query.materialize_view(@w)
    indexes << view

    planner = Planner.new @w, indexes
    tree = planner.find_plans_for_query @query
    expect(tree.min).to match_array [IndexLookupStep.new(view)]
  end
end
