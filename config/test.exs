import Config

config :logger, level: :debug

config :cue, Cue.TestRepo, migration_timestamps: [type: :utc_datetime_usec]
