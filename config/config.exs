import Config

config :cue, ecto_repos: [Cue.TestRepo]

config :cue, Cue.TestRepo,
  username: "postgres",
  password: "postgres",
  database: "cue_test",
  hostname: "localhost",
  port: 5432,
  pool: Ecto.Adapters.SQL.Sandbox

if config_env() in [:test] do
  import_config "#{config_env()}.exs"
end
