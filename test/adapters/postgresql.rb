ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  database: "groupdate_test",
  host: "localhost",
  port: 5432,
  username: "groupdate",
  password: "groupdate"
)
