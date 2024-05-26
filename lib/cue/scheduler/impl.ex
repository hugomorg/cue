defmodule Cue.Scheduler.Impl do
  @moduledoc """
  Responsible for running the jobs roughly on their schedule. 
  """

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Scheduler
  alias Cue.Job
  @repo Application.compile_env!(:cue, :repo)

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

    {:ok, %{ignore_jobs: [], paused?: false, run_once?: !!args[:run_once]}}
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
  def handle_info(_, state = %{paused?: true}) do
    {:noreply, state}
  end

  @stale_job_padding_millis 1000 * 60

  @impl GenServer
  def handle_info(:check, state) do
    loop(:check, @check_interval)

    timeout = Application.get_env(:cue, :timeout, 5000)

    Job
    |> where([j], ^DateTime.utc_now() >= j.run_at)
    |> where([j], is_nil(j.max_retries) or j.retry_count < j.max_retries)
    |> where(
      [j],
      j.status == :not_started or
        ((not is_nil(j.schedule) and j.status in [:failed, :succeeded]) or
           (j.status == :processing and
              j.run_at <
                ^DateTime.add(
                  DateTime.utc_now(),
                  -(timeout + @stale_job_padding_millis),
                  :millisecond
                )))
    )
    |> order_by(:run_at)
    |> @repo.all()
    |> Enum.filter(&(&1.name not in state.ignore_jobs))
    |> Cue.Processor.impl().process_jobs()

    {:noreply, if(state.run_once?, do: %{state | paused?: true}, else: state)}
  end

  ## Helpers
  defp loop(task, milliseconds) when is_integer(milliseconds) do
    Process.send_after(self(), task, milliseconds)
  end
end
