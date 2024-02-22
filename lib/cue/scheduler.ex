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
    {:ok, %{ignore_jobs: []}}
  end

  def add_job_to_ignored(job_name) do
    GenServer.call(__MODULE__, {:add_job_to_ignored, job_name})
  end

  def remove_job_from_ignored(job_name) do
    GenServer.call(__MODULE__, {:remove_job_from_ignored, job_name})
  end

  @impl true
  def handle_call({:add_job_to_ignored, job_name}, _from, state) do
    {:reply, :ok, %{state | ignore_jobs: [job_name | state.ignore_jobs]}}
  end

  @impl true
  def handle_call({:remove_job_from_ignored, job_name}, _from, state) do
    {:reply, :ok, %{state | ignore_jobs: Enum.reject(state.ignore_jobs, &(&1 == job_name))}}
  end

  # Scheduling logic
  @impl true
  def handle_info(:check, state) do
    loop(:check, @check_every_seconds)
    jobs = Job |> where([j], ^DateTime.utc_now() >= j.run_at) |> order_by(:run_at) |> @repo.all()

    for job <- jobs, job.name not in state.ignore_jobs do
      Cue.Processor.process_job(job)
    end

    {:noreply, state}
  end

  defp loop(task, seconds) do
    Process.send_after(self(), task, :timer.seconds(seconds))
  end
end
