var Table = require('mysql-faker').Table,
    insert = require('mysql-faker').insert;

var categories = (new Table('categories', 500));
categories.lorem_words('name', 2);

var regions = (new Table('regions', 50));
regions.lorem_words('name', 2);

var users = (new Table('users', 200000));
users.name_firstName('firstname')
     .name_lastName('lastname')
     .random_uuid('nickname')
     .internet_password('password')
     .internet_email('email')
     .random_number('rating', {min: -50, max: 200})
     .finance_amount('balance')
     .date_past('creation_date')
     .random_number('region', {min: 1, max: regions.count});

var items = (new Table('items', 2000000));
items.lorem_words('name')
     .lorem_paragraph('description')
     .finance_amount('initial_price')
     .random_number('quantity', {min: 0, max: 10})
     .finance_amount('reserve_price')
     .finance_amount('buy_now')
     .random_number('nb_of_bids', {min: 0, max: 100})
     .finance_amount('max_bid')
     .date_past('start_date')
     .date_past('end_date')
     .random_number('seller', {min: 1, max: users.count})
     .random_number('category', {min: 1, max: categories.count});

var bids = (new Table('bids', 20000000));
bids.random_number('qty', {min: 1, max: 5})
    .finance_amount('bid')
    .finance_amount('max_bid')
    .date_past('date')
    .random_number('user', {min: 1, max: users.count})
    .random_number('item', {min: 1, max: items.count});

var comments = (new Table('comments', 10000000));
comments.random_number('rating', {min: -5, max: 5})
        .date_past('date')
        .lorem_sentences('comment')
        .random_number('from_user', {min: 1, max: users.count})
        .random_number('to_user', {min: 1, max: users.count})
        .random_number('item', {min: 1, max: items.count});

var buy_now = (new Table('buynow', 2000000));
buy_now.random_number('qty', {min: 1, max: 3})
       .date_past('date')
       .random_number('buyer', {min: 1, max: users.count})
       .random_number('item', {min: 1, max: users.count});

insert([
  categories,
  regions,
  users,
  items,
  bids,
  comments,
  buy_now
], {
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'rubis'
}, true);
