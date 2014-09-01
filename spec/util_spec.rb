require 'stringio'

describe Enumerable do
  it 'can generate all possible prefixes' do
    expect([1, 2, 3].prefixes).to match_array [[1], [1, 2], [1, 2, 3]]
  end

  it 'generates an empty prefix list when there are no elements' do
    expect([].prefixes).to match_array []
  end

  it 'can generate all partitionings of itself' do
    expect([1, 2, 3, 4].partitions).to match_array [
      [[1], [2, 3, 4]],
      [[1, 2], [3, 4]],
      [[1, 2, 3], [4]]]
  end
end

describe Fixnum do
  it 'is finite' do
    expect(3.finite?).to be true
  end
end

describe Object do
  context 'for objects with a to_color method' do
    subject(:obj) do
      class Foo
        def to_color
          'foo'.red
        end
      end

      Foo.new
    end

    it 'should inspect colored output when stdout is a terminal' do
      $stdout = double('stdout', isatty: true, write: nil)
      expect(obj.inspect).to eq "\e[0;31;49mfoo\e[0m"
      $stdout = STDOUT
    end

    it 'should inspect uncolored output when stdout is not a terminal' do
      $stdout = StringIO.new
      expect(obj.inspect).to eq 'foo'
      $stdout = STDOUT
    end
  end

  context 'for objects without a to_color method' do
    subject(:obj) do
      class Bar
        def to_s
          'foo'
        end
      end

      Bar.new
    end

    it 'should use uncolored output' do
      expect(obj.to_s).to eq 'foo'
      expect(obj.to_color).to eq 'foo'
    end
  end
end
