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
      [[1, 2, 3, 4], []],
      [[1], [2, 3, 4]],
      [[1, 2], [3, 4]],
      [[1, 2, 3], [4]]]
  end

  it 'can compute the product based on a block' do
    expect([-1, 1].sum_by(&:abs)).to eq(2)
  end

  it 'can compute the product based on a block' do
    expect([-1, 1].product_by(&:abs)).to eq(1)
  end
end

describe Integer do
  it 'is finite' do
    expect(3.finite?).to be true
  end
end

describe Object do
  context 'for objects with a to_color method' do
    subject(:obj) do
      class Foo
        def to_color
          '[red]foo[/]'
        end
      end

      Foo.new
    end

    it 'should inspect colored output when stdout is a terminal' do
      old_stdout = STDOUT
      Object.instance_eval { remove_const 'STDOUT' }
      STDOUT = double('stdout', tty?: true, write: nil)

      expect(obj.inspect).to eq "\e[31mfoo\e[0m"

      Object.instance_eval { remove_const 'STDOUT' }
      STDOUT = old_stdout
    end

    it 'should inspect uncolored output when stdout is not a terminal' do
      old_stdout = STDOUT
      Object.instance_eval { remove_const 'STDOUT' }
      STDOUT = StringIO.new

      Object.instance_eval { remove_const 'STDOUT' }
      STDOUT = old_stdout
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

  describe 'Subtype' do
    subject(:obj) do
      class Foo
      end

      class BarBazFoo < Foo
        include Subtype
      end

      BarBazFoo.new
    end

    it 'can produce its name in snake case' do
      expect(obj.subtype_name).to eq 'bar_baz'
    end

    it 'can produce its name in camel case' do
      expect(obj.subtype_name(name_case: :camel)).to eq 'BarBaz'
    end
  end

  describe 'Supertype' do
    subject(:cls) do
      class Foo
        include Supertype
      end

      class BarBazFoo < Foo
      end

      Foo
    end

    it 'can produce a subclass from a name in snake case' do
      subclass = cls.subtype_class 'bar_baz'
      expect(subclass).to be_a Class
      expect(subclass.name).to eq 'BarBazFoo'
    end

    it 'can produce a subclass from a name in camel case' do
      subclass = cls.subtype_class 'BarBaz'
      expect(subclass).to be_a Class
      expect(subclass.name).to eq 'BarBazFoo'
    end
  end
end

describe Cardinality do
  include_context 'entities'

  it 'estimates one for a simple ID lookup' do
    cardinality = Cardinality.filter tweet.count, [tweet['TweetId']], nil

    expect(cardinality).to eq(1)
  end

  it 'correctly estimates based on field cardinality for equality' do
    cardinality = Cardinality.filter user.count, [user['City']], nil

    expect(cardinality).to eq(2)
  end

  it 'uses a static estimate for range filters' do
    cardinality = Cardinality.filter tweet.count, [tweet['Body']],
                                     tweet['Timestamp']

    expect(cardinality).to eq(20)
  end
end

describe Listing do
  let(:superclass) do
    class Super
      include Listing
    end
  end

  let(:subclass) do
    class Sub < Super
    end

    Sub
  end

  it 'allows tracking of subclasses' do
    expect(superclass.subclasses).to eq({"Sub" => subclass})
  end
end
