import Config

config :cue, :scheduler, Cue.Scheduler

import_config "#{config_env()}.exs"
