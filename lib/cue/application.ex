defmodule Cue.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [Cue.Processor, {Task.Supervisor, name: Cue.TaskProcessor}, Cue.Scheduler]

    children =
      if Mix.env() == :test do
        [Cue.TestRepo] ++ children
      else
        children
      end

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Cue.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
