defmodule Cue.Scheduler.Impl do
  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Scheduler
  alias Cue.Job
  @repo Application.compile_env(:cue, :repo)

  @check_interval Application.compile_env(:cue, :check_interval, 1000)

  @behaviour Scheduler

  ## GenServer interface
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  @impl Scheduler
  # We want to temporarily ignore certain jobs if they are getting removed
  def add_job_to_ignored(job_name) do
    GenServer.call(__MODULE__, {:add_job_to_ignored, job_name})
  end

  @impl Scheduler
  def add_jobs_to_ignored(job_names) do
    GenServer.call(__MODULE__, {:add_jobs_to_ignored, job_names})
  end

  @impl Scheduler
  def remove_job_from_ignored(job_name) do
    GenServer.call(__MODULE__, {:remove_job_from_ignored, job_name})
  end

  @impl Scheduler
  def remove_jobs_from_ignored(job_names) do
    GenServer.call(__MODULE__, {:remove_jobs_from_ignored, job_names})
  end

  @impl Scheduler
  def pause do
    GenServer.call(__MODULE__, :pause)
  end

  @impl Scheduler
  def resume do
    GenServer.call(__MODULE__, :resume)
  end

  @impl GenServer
  def init(args) do
    loop(:check, @check_interval)

    {:ok, %{ignore_jobs: [], paused?: false}}
  end

  @impl GenServer
  def handle_call({:add_job_to_ignored, job_name}, _from, state) do
    {:reply, :ok, %{state | ignore_jobs: [job_name | state.ignore_jobs]}}
  end

  @impl GenServer
  def handle_call({:add_jobs_to_ignored, job_names}, _from, state) do
    {:reply, :ok, %{state | ignore_jobs: job_names ++ state.ignore_jobs}}
  end

  @impl GenServer
  def handle_call({:remove_job_from_ignored, job_name}, _from, state) do
    {:reply, :ok, %{state | ignore_jobs: Enum.reject(state.ignore_jobs, &(&1 == job_name))}}
  end

  @impl GenServer
  def handle_call({:remove_jobs_from_ignored, job_names}, _from, state) do
    {:reply, :ok, %{state | ignore_jobs: state.ignore_jobs -- job_names}}
  end

  @impl GenServer
  def handle_call(:pause, _from, state) do
    {:reply, :ok, %{state | paused?: true}}
  end

  @impl GenServer
  def handle_call(:resume, _from, state) do
    {:reply, :ok, %{state | paused?: false}}
  end

  @impl GenServer
  def handle_info(:check, state = %{paused?: true}) do
    loop(:check, @check_interval)

    {:noreply, state}
  end

  # Scheduling logic
  @impl GenServer
  def handle_info(:check, state) do
    loop(:check, @check_interval)

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
    |> Cue.Processor.impl().process_jobs()

    {:noreply, state}
  end

  ## Helpers
  defp loop(task, milliseconds) when is_integer(milliseconds) do
    Process.send_after(self(), task, milliseconds)
  end
end
