import Config

config :logger, level: :info

config :cue, ecto_repos: [Cue.TestRepo]

config :cue, Cue.TestRepo,
  username: "postgres",
  password: "postgres",
  database: "cue_test",
  hostname: "localhost",
  port: 5432,
  pool: Ecto.Adapters.SQL.Sandbox

config :cue, Cue.TestRepo, migration_timestamps: [type: :utc_datetime_usec]
config :cue, repo: Cue.TestRepo
