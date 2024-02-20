import Config

if config_env() in [:test, :dev] do
  import_config "#{config_env()}.exs"
end
