defmodule Cue.Scheduler do
  @moduledoc false

  @type job_name :: String.t()

  @callback add_job_to_ignored(job_name()) :: :ok
  @callback add_jobs_to_ignored([job_name()]) :: :ok
  @callback remove_job_from_ignored(job_name()) :: :ok
  @callback remove_jobs_from_ignored([job_name()]) :: :ok
  @callback pause() :: :ok
  @callback resume() :: :ok

  @implementation Application.compile_env!(:cue, :scheduler)

  def impl, do: @implementation

  defmodule Impl do
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

    def add_jobs_to_ignored(job_names) do
      GenServer.call(__MODULE__, {:add_jobs_to_ignored, job_names})
    end

    def remove_job_from_ignored(job_name) do
      GenServer.call(__MODULE__, {:remove_job_from_ignored, job_name})
    end

    def remove_jobs_from_ignored(job_names) do
      GenServer.call(__MODULE__, {:remove_jobs_from_ignored, job_names})
    end

    def pause do
      GenServer.call(__MODULE__, :pause)
    end

    def resume do
      GenServer.call(__MODULE__, :resume)
    end

    if @repo do
      ## GenServer callbacks
      @impl true
      def init(_args) do
        loop(:check, @check_every_seconds)

        {:ok, %{ignore_jobs: [], paused?: false}}
      end
    else
      @impl true
      def init(_args) do
        Logger.warn("No repo, scheduler will not loop")
        {:ok, %{ignore_jobs: [], paused?: false}}
      end
    end

    @impl true
    def handle_call({:add_job_to_ignored, job_name}, _from, state) do
      {:reply, :ok, %{state | ignore_jobs: [job_name | state.ignore_jobs]}}
    end

    @impl true
    def handle_call({:add_jobs_to_ignored, job_names}, _from, state) do
      {:reply, :ok, %{state | ignore_jobs: job_names ++ state.ignore_jobs}}
    end

    @impl true
    def handle_call({:remove_job_from_ignored, job_name}, _from, state) do
      {:reply, :ok, %{state | ignore_jobs: Enum.reject(state.ignore_jobs, &(&1 == job_name))}}
    end

    @impl true
    def handle_call({:remove_jobs_from_ignored, job_names}, _from, state) do
      {:reply, :ok, %{state | ignore_jobs: state.ignore_jobs -- job_names}}
    end

    @impl true
    def handle_call(:pause, _from, state) do
      {:reply, :ok, %{state | paused?: true}}
    end

    @impl true
    def handle_call(:resume, _from, state) do
      {:reply, :ok, %{state | paused?: false}}
    end

    @impl true
    def handle_info(:check, state = %{paused?: true}) do
      loop(:check, @check_every_seconds)

      {:noreply, state}
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
end
