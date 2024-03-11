defmodule Cue.Scheduler do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Job
  @repo Application.compile_env(:cue, :repo)

  @check_every_seconds 1

  ## GenServer interface
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  # We want to temporarily ignore certain jobs if they are getting removed
  def add_job_to_ignored(job_name) do
    GenServer.call(__MODULE__, {:add_job_to_ignored, job_name})
  end

  def remove_job_from_ignored(job_name) do
    GenServer.call(__MODULE__, {:remove_job_from_ignored, job_name})
  end

  if @repo do
    ## GenServer callbacks
    @impl true
    def init(_args) do
      loop(:check, @check_every_seconds)

      {:ok, %{ignore_jobs: []}}
    end
  else
    @impl true
    def init(_args) do
      Logger.warn("No repo, scheduler will not loop")
      {:ok, %{ignore_jobs: []}}
    end
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

    Job
    |> where([j], ^DateTime.utc_now() >= j.run_at)
    # for one-off jobs
    |> where(
      [j],
      (not is_nil(j.schedule) and (is_nil(j.max_retries) or j.retry_count < j.max_retries)) or
        j.status == :not_started
    )
    |> order_by(:run_at)
    |> @repo.all()
    |> Enum.filter(&(&1.name not in state.ignore_jobs))
    |> Cue.Processor.process_jobs()

    {:noreply, state}
  end

  ## Helpers
  defp loop(task, seconds) do
    Process.send_after(self(), task, :timer.seconds(seconds))
  end
end
