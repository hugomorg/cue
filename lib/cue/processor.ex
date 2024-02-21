defmodule Cue.Processor do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Schemas.Job
  @repo Application.compile_env!(:cue, :repo)

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  @impl true
  def init(_) do
    {:ok, %{refs: %{}}}
  end

  # Processing logic
  def process_job(job) do
    GenServer.call(__MODULE__, {:start_task, nil, job})
  end

  def remove_job(name) do
    GenServer.call(__MODULE__, {:remove_task, name})
  end

  @impl true
  # In this case the task is already running, so we just return :ok.
  def handle_call({:start_task, ref, _job}, _from, %{refs: refs} = state)
      when is_map_key(refs, ref) do
    {:reply, :ok, state}
  end

  @impl true
  # The task is not running yet, so let's start it.
  def handle_call({:start_task, _ref, job}, _from, %{refs: refs} = state) do
    task =
      Task.Supervisor.async_nolink(Cue.TaskProcessor, fn ->
        handle_job(job)
      end)

    # We return :ok and the server will continue running
    {:reply, :ok, %{state | refs: Map.put(refs, task.ref, %{task: task, job: job})}}
  end

  # def handle_call(call, from, state) do
  #   IO.inspect("call=#{inspect(call)}")
  #   {:reply, :ok, state}
  # end

  @impl true
  # In this case the task is already running, so we just return :ok.
  def handle_call({:remove_task, job_name}, _from, %{refs: %{} = refs} = state) do
    refs =
      case Enum.find(refs, fn {_ref, %{job: job}} -> job.name == job_name end) do
        {ref, _value} ->
          Logger.debug("#{job_name} removed from processing")
          Map.delete(refs, ref)

        nil ->
          Logger.debug("no job found for #{job_name} to delete")
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
  def handle_info({ref, {:ok, _job}}, %{refs: refs} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | refs: Map.delete(refs, ref)}}
  end

  # The task failed
  def handle_info({:DOWN, ref, :process, _pid, error}, %{refs: refs} = state)
      when is_map_key(refs, ref) do
    %{job: job} = refs[ref]
    # TODO: add retry logic here
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
    handler = validate_handler!(job.handler)

    case apply_handler(handler, job) do
      {:error, error} ->
        update_job_as_failed!(job, error)

      {:ok, context} ->
        update_job_as_success!(job, context)

      :ok ->
        update_job_as_success!(job, job.context)
    end

    {:ok, job}
  end

  defp update_job_as_failed!(job, error) do
    now = DateTime.utc_now()
    retry_count = job.retry_count + 1

    status =
      if is_integer(job.max_retries) and retry_count >= job.max_retries do
        :paused
      else
        :failed
      end

    job
    |> Job.changeset(%{
      last_failed_at: now,
      last_error: inspect(error),
      run_at: Job.next_run_at(job),
      retry_count: job.retry_count + 1,
      status: status
    })
    |> @repo.update!
  end

  defp update_job_as_success!(job, context) do
    now = DateTime.utc_now()

    job
    |> Job.changeset(%{
      last_succeeded_at: now,
      run_at: Job.next_run_at(job),
      context: context,
      retry_count: 0,
      status: :succeeded
    })
    |> @repo.update!
  end

  defp validate_handler!(handler) when is_atom(handler) do
    validate_handler!({handler, :handle_job})
  end

  defp validate_handler!({module, fun}) when is_atom(module) and is_atom(fun) do
    module_exists? = Code.ensure_loaded?(module)

    cond do
      module_exists? and function_exported?(module, fun, 1) -> {module, fun}
      module_exists? -> raise "#{inspect(fun)} is not defined"
      :else -> "handler #{inspect(module)} not a module"
    end
  end

  defp apply_handler({module, fun}, job) do
    apply(module, fun, [job])
  end

  defp apply_handler(handler, job) do
    apply_handler({handler, :handle_job}, job)
  end

  defp maybe_apply_error_handler(job) do
    case job.error_handler do
      {module, fun} -> validate_handler!({module, fun})
      handler -> validate_handler!({handler, :handle_job_error})
    end
    |> apply_handler(job)
  end
end
