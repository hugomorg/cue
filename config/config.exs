import Config

config :cue, :scheduler, Cue.Scheduler.Impl
config :cue, :processor, Cue.Processor.Impl

import_config "#{config_env()}.exs"
