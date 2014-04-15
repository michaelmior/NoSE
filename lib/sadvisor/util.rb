module Enumerable
  def prefixes
    Enumerator.new do |enum|
      prefix = []
      each do |elem|
        prefix = prefix.dup << elem
        enum.yield prefix
      end
    end
  end

  def product(other)
    Enumerator.new do |enum|
      other.each { |b| enum.yield [[], b] }
      each do |a|
        enum.yield [a, []]

        other.each { |b| enum.yield [a, b] }
      end
    end
  end
end
