import Config

config :cue, :scheduler, Cue.Scheduler
config :cue, :processor, Cue.Processor

import_config "#{config_env()}.exs"
