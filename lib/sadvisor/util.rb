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
end
