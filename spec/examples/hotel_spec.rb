require 'sadvisor'

describe 'Hotel example' do
  before(:each) do
    @w = w = Workload.new

    @w << Entity.new('Hotel') do
      ID 'HotelID'
      String 'Name', 20
      String 'Phone', 10
      String 'Address', 50
      String 'City', 20
      String 'Zip', 5
    end

    @w << Entity.new('Room') do
      ID 'RoomID'
      ForeignKey 'HotelID', w['Hotel']
      String 'RoomNumber', 4
      Float 'Rate'
    end

    @w << Entity.new('Guest') do
      ID 'GuestID'
      String 'Name', 20
      String 'Email', 20
    end

    @w << Entity.new('Reservation') do
      ID 'ReservationID'
      ForeignKey 'GuestID', w['Guest']
      Date 'StartDate'
      Date 'EndDate'
    end

    @w << Entity.new('POI') do
      ID 'POIID'
      String 'Name', 20
      String 'Description', 200
    end

    @w << Entity.new('HotelToPOI') do
      ForeignKey 'HotelID', w['Hotel']
      ForeignKey 'POIID', w['POI']
    end

    @w << Entity.new('Amenity') do
      ID 'AmenityID'
      String 'Name', 20
    end

    @w << Entity.new('RoomToAmenity') do
      ForeignKey 'RoomID', w['Room']
      ForeignKey 'AmenityID', w['Amenity']
    end
    @query = Parser.parse 'SELECT Name FROM POI WHERE ' \
                          'Hotel.Room.Reservation.Guest.GuestID = 3'
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
      @w['POI'] => [[@w['POI']['POIID']]],
      @w['Guest'] => [[@w['Guest']['GuestID']],
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
end
