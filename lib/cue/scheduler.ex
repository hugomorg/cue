defmodule Cue.Scheduler do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Schemas.Job
  @repo Cue.TestRepo

  @check_every_seconds 1

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  @impl true
  def init(_args) do
    loop(:check, @check_every_seconds)
    {:ok, %{}}
  end

  # Scheduling logic
  @impl true
  def handle_info(:check, state) do
    loop(:check, @check_every_seconds)
    jobs = Job |> where([j], ^DateTime.utc_now() >= j.run_at) |> @repo.all()

    for job <- jobs do
      Cue.Processor.process_job(job)
    end

    {:noreply, state}
  end

  defp loop(task, seconds) do
    Process.send_after(self(), task, :timer.seconds(seconds))
  end
end
