defmodule Cue.Processor do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Job
  @repo Application.compile_env!(:cue, :repo)

  ## GenServer interface
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  def process_job(job) do
    GenServer.call(__MODULE__, {:start_task, nil, job})
  end

  def remove_job(name) do
    GenServer.call(__MODULE__, {:remove_task, name})
  end

  ## GenServer callbacks
  @impl true
  def init(_) do
    {:ok, %{refs: %{}}}
  end

  @impl true
  # Job is already running
  def handle_call({:start_task, ref, _job}, _from, %{refs: refs} = state)
      when is_map_key(refs, ref) do
    {:reply, :ok, state}
  end

  @impl true
  # Start processing job - not running
  def handle_call({:start_task, _ref, job}, _from, %{refs: refs} = state) do
    task =
      Task.Supervisor.async_nolink(Cue.TaskProcessor, fn ->
        handle_job(job)
      end)

    {:reply, :ok, %{state | refs: Map.put(refs, task.ref, %{task: task, job: job})}}
  end

  @impl true
  # In this case the task is already running, so we just return :ok.
  def handle_call({:remove_task, job_name}, _from, %{refs: %{} = refs} = state) do
    refs =
      case Enum.find(refs, fn {_ref, %{job: job}} -> job.name == job_name end) do
        {ref, _value} ->
          Logger.debug("Job #{job_name} removed from processor")
          Map.delete(refs, ref)

        nil ->
          Logger.warning(
            "Attempting to remove the task, but no job found for #{job_name} to delete"
          )

          refs
      end

    {:reply, :ok, %{state | refs: refs}}
  end

  @impl true
  # Skipping because of lock
  def handle_info({ref, {:ok, :locked}}, %{refs: refs} = state) do
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | refs: Map.delete(refs, ref)}}
  end

  @impl true
  # The task completed successfully
  def handle_info({ref, {:ok, job}}, %{refs: refs} = state) do
    Logger.debug("Job #{job.name} was handled OK")
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | refs: Map.delete(refs, ref)}}
  end

  # The task failed
  def handle_info({:DOWN, ref, :process, _pid, error}, %{refs: refs} = state)
      when is_map_key(refs, ref) do
    %{job: job} = refs[ref]
    # TODO: add retry logic here
    Logger.error("Job #{job.name} failed error=#{inspect(error)}")
    update_job_as_failed!(job, error)
    maybe_apply_error_handler(job)
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | refs: Map.delete(refs, ref)}}
  end

  # Key not in map means in all likelihood task has been deleted
  def handle_info({:DOWN, ref, :process, _pid, _error}, state) do
    Logger.warn("No task found - probably deleted")
    Process.demonitor(ref, [:flush])
    {:noreply, state}
  end

  ## Processing logic
  def handle_job(job) do
    Logger.debug("handling job=#{inspect(job)}")

    with {:ok, %{job: job}} <-
           Ecto.Multi.new()
           |> Ecto.Multi.one(
             :free_job,
             Job
             |> where([j], j.id == ^job.id and j.status != :processing and j.status != :paused)
             |> lock("FOR UPDATE SKIP LOCKED")
           )
           |> Ecto.Multi.run(:job, fn _repo, changes -> maybe_handle_job(changes) end)
           |> @repo.transaction() do
      {:ok, job}
    end
  end

  defp maybe_handle_job(%{free_job: nil}), do: {:ok, :locked}

  defp maybe_handle_job(changes) do
    job = changes.free_job |> Job.changeset(%{status: :processing}) |> @repo.update!

    case apply(job.handler, :handle_job, [job.name, job.state]) do
      {:error, error} ->
        maybe_apply_error_handler(job)
        update_job_as_failed!(job, error)

      {:ok, state} ->
        update_job_as_success!(job, state)

      :ok ->
        update_job_as_success!(job, job.state)
    end

    {:ok, job}
  end

  defp update_job_as_failed!(job, error) do
    now = DateTime.utc_now()
    retry_count = job.retry_count + 1

    status =
      if Job.retries_exceeded?(%Job{job | retry_count: retry_count}) do
        :paused
      else
        :failed
      end

    job
    |> Job.changeset(%{
      last_failed_at: now,
      last_error: inspect(error),
      run_at: Job.next_run_at!(job),
      retry_count: job.retry_count + 1,
      status: status
    })
    |> @repo.update!
  end

  defp update_job_as_success!(job, state) do
    now = DateTime.utc_now()

    job
    |> Job.changeset(%{
      last_succeeded_at: now,
      run_at: Job.next_run_at!(job),
      state: state,
      retry_count: 0,
      status: :succeeded
    })
    |> @repo.update!
  end

  defp maybe_apply_error_handler(job) do
    apply(job.handler, :handle_job_error, [
      job.name,
      job.state,
      %{error: job.last_error, retry_count: job.retry_count, max_retries: job.max_retries}
    ])
  end
end
