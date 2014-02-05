user = Entity.new('User')
user << IDField.new('UserId')
user << StringField.new('Password', 10)
user << StringField.new('Username', 10)

user << ToManyKey.new('follows', user)

tweet = Entity.new('Tweet')
tweet << IDField.new('TweetId')
tweet << StringField.new('Body', 100)
tweet << TimestampField.new('Timestamp')

tweet << ToOneKey.new('author', user)
