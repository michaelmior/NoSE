describe 'Hotel example' do
  before(:each) do
    @w = w = Sadvisor::Workload.new

    @w << Sadvisor::Entity.new('POI') do
      ID 'POIID'
      String 'Name', 20
      String 'Description', 200
    end

    @w << Sadvisor::Entity.new('Hotel') do
      ID 'HotelID'
      String 'Name', 20
      String 'Phone', 10
      String 'Address', 50
      String 'City', 20
      String 'Zip', 5
    end

    @w << Sadvisor::Entity.new('HotelToPOI') do
      ID 'ID'
      ForeignKey 'HotelID', w['Hotel']
      ForeignKey 'POIID', w['POI']
    end

    @w << Sadvisor::Entity.new('Amenity') do
      ID 'AmenityID'
      String 'Name', 20
    end

    @w << Sadvisor::Entity.new('Room') do
      ID 'RoomID'
      ForeignKey 'HotelID', w['Hotel']
      String 'RoomNumber', 4
      Float 'Rate'
      ToManyKey 'Amenities', w['Amenity']
    end

    @w << Sadvisor::Entity.new('Guest') do
      ID 'GuestID'
      String 'Name', 20
      String 'Email', 20
    end

    @w << Sadvisor::Entity.new('Reservation') do
      ID 'ReservationID'
      ForeignKey 'GuestID', w['Guest']
      ForeignKey 'RoomID', w['Room']
      Date 'StartDate'
      Date 'EndDate'
    end

    @query = Sadvisor::Parser.parse 'SELECT Name FROM POI WHERE ' \
                                    'POI.Hotel.Room.Reservation.' \
                                    'Guest.GuestID = 3'
    @w.add_query @query
  end

  it 'can look up entities via multiple foreign keys' do
    guest_id = @w['Guest']['GuestID']
    index = Sadvisor::Index.new([guest_id], [@w['POI']['Name']], [
      @w['Guest'], @w['Reservation'], @w['Room'], @w['Hotel'], @w['POI']
    ])
    index.set_field_keys guest_id, \
                         [@w['Hotel']['Room'],
                          @w['Room']['Reservation'],
                          guest_id]
    planner = Sadvisor::Planner.new @w, [index]
    tree = planner.find_plans_for_query @query
    expect(tree).to have(1).plan
    expect(tree).to include [Sadvisor::IndexLookupStep.new(index)]
  end

  it 'uses the workload to find foreign key traversals' do
    fields = @w.find_field_keys %w(Hotel Room Reservation Guest GuestID)
    expect(fields).to eq \
        [[@w['Guest']['GuestID']],
         [@w['Reservation']['ReservationID']],
         [@w['Room']['RoomID']],
         [@w['Hotel']['HotelID']]]
  end

  it 'can look up entities using multiple indices' do
    simple_indexes = @w.entities.values.map(&:simple_index)
    planner = Sadvisor::Planner.new @w, simple_indexes
    tree = planner.find_plans_for_query @query
    expect(tree).to include [
      Sadvisor::IndexLookupStep.new(@w['Reservation'].simple_index),
      Sadvisor::IndexLookupStep.new(@w['Room'].simple_index),
      Sadvisor::IndexLookupStep.new(@w['Hotel'].simple_index),
      Sadvisor::IndexLookupStep.new(@w['POI'].simple_index),
      Sadvisor::FilterStep.new([@w['Guest']['GuestID']], nil)]
  end

  it 'can select from multiple plans' do
    indexes = @w.entities.values.map(&:simple_index)
    view = @query.materialize_view(@w)
    indexes << view

    planner = Sadvisor::Planner.new @w, indexes
    tree = planner.find_plans_for_query @query
    expect(tree.size).to be > 1
    expect(tree.min).to match_array [Sadvisor::IndexLookupStep.new(view)]
  end

  it 'can search for an optimal index by checking non-overlapping indexes' do
    indexes = Sadvisor::Search.new(@w).search_overlap 1000
    expect(indexes).to match_array [
      Sadvisor::Index.new([@w['Guest']['GuestID']], [@w['POI']['Name']], [])
    ]
  end
end
