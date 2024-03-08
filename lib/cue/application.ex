defmodule Cue.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [{Task.Supervisor, name: Cue.TaskProcessor}, Cue.Scheduler]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Cue.Supervisor]

    children
    |> maybe_start_test_repo(Mix.env())
    |> Supervisor.start_link(opts)
  end

  defp maybe_start_test_repo(children, env) when env in [:test] do
    [Cue.TestRepo] ++ children
  end

  defp maybe_start_test_repo(children, _env) do
    children
  end
end
