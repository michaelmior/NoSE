require 'erb'
require 'ostruct'
require 'tempfile'

template = <<EOF
# Total storage capacity
param T;

# Benefit of queries for configuration and configuration storage
set b, dimen 3;
set C, dimen 2;
set S, dimen 2;

# Indices
set Q := setof{(i,j,c) in b : j == 1} i;
set I := setof{(i,s) in S} i;
set E := setof{(i,j) in Q cross I} (i, j);

# Assignment
var x{(i,j) in E}, >=0, <=1, binary;
var y{I}, >=0, <=1, binary;

maximize obj :
  sum{(i,j,c) in b} x[i,j] * c;

s.t. size :
  sum{(i,c) in C} c * y[i] <= T;

<% 1.upto(benefits.count) do |i| %>
s.t. q<%= i %>config :
  sum{(i,s) in S} x[<%= i %>,i] <= 1;

<% end %>

<%
benefits.each_with_index do |qb, i|
  qb.each_with_index do |b, j|
    next unless b > 0
    configurations[j].each do |index| %>
s.t. q<%= i + 1 %><%= j + 1 %><%= index %>avail :
  x[<%= i + 1 %>,<%= j + 1 %>] <= y[<%= index %>];

<%
    end
  end
end %>

solve;

printf "The configuration contains:\\n";
printf {(i,s) in S: y[i] == 1} " %i", i;
printf "\\nThe configuration does not contain:\\n";
printf {(i,s) in S: y[i] == 0} " %i", i;
printf "\\n";

data;

# Total storage capacity
param T := <%= T %>;

set C :=
<% index_sizes.each_with_index do |size, i| %>
  <%= i + 1 %> <%= size %>

<% end %>;

set S :=
<% configuration_sizes.each_with_index do |size, i| %>
  <%= i + 1 %> <%= size %>

<% end %>;

set b :=
<%
benefits.each_with_index do |qb, i|
  qb.each_with_index do |b, j| %>
  <%= i + 1 %> <%= j + 1 %> <%= b %>

<%
  end
end %>;

end;
EOF

class Namespace
  def initialize(hash)
    hash.each do |key, value|
      singleton_class.send(:define_method, key) { value }
    end 
  end

  def get_binding
    binding
  end
end

T = 200
index_sizes = [100, 100, 100, 100]
configurations = [[1], [2], [1, 2], [3], [4], [3, 4]]
configuration_sizes = [100, 100, 200, 100, 100, 200]
benefits = [[30, 30, 75, 0, 0, 0],
            [0, 0, 0, 35, 35, 73]]

namespace = Namespace.new({T: T, benefits: benefits, configurations: configurations, index_sizes: index_sizes, configuration_sizes: configuration_sizes})
mpl = ERB.new(template, 0, '>').result(namespace.get_binding)

require 'glpk_wrapper'

file = Tempfile.new 'mpl'
begin
  file.write mpl
  file.close

  mip = Glpk_wrapper.glp_create_prob
  tran = Glpk_wrapper.glp_mpl_alloc_wksp

  ret = Glpk_wrapper.glp_mpl_read_model tran, file.path, 0
  ret = Glpk_wrapper.glp_mpl_generate tran, nil
  Glpk_wrapper.glp_mpl_build_prob tran, mip
  Glpk_wrapper.glp_simplex mip, nil
  Glpk_wrapper.glp_intopt mip, nil
  ret = Glpk_wrapper.glp_mpl_postsolve tran, mip, Glpk_wrapper::GLP_MIP
  Glpk_wrapper.glp_mpl_free_wksp tran
  Glpk_wrapper.glp_delete_prob mip
ensure
  file.close
  file.unlink
end
