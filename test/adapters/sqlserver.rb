ActiveRecord::Base.establish_connection(
  adapter: "sqlserver",
  database: "groupdate_test",
  host: "localhost",
  port: 1433,
  username: "SA",
  password: "yourStrongPassword123"
)
