defmodule Cue.Processor do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Schemas.Job
  @repo Cue.TestRepo

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

  @impl true
  # In this case the task is already running, so we just return :ok.
  def handle_call({:start_task, ref, job}, _from, %{refs: refs} = state)
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
  # Skipping because of lock
  def handle_info({ref, {:ok, :locked}}, state) do
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])
    {:noreply, state}
  end

  @impl true
  # The task completed successfully
  def handle_info({ref, {:ok, job}}, %{refs: refs} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | refs: Map.delete(refs, ref)}}
  end

  # The task failed
  def handle_info({:DOWN, ref, :process, _pid, error}, %{refs: refs} = state) do
    %{job: job} = Map.fetch!(refs, ref)
    update_job_as_failed(job, error)
    # process_job(job)
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | refs: Map.delete(refs, ref)}}
  end

  def handle_job(job) do
    Logger.debug("handling job=#{inspect(job)}")

    with {:ok, %{job: job}} <-
           Ecto.Multi.new()
           |> Ecto.Multi.one(
             :lock,
             Job
             |> where([j], j.id == ^job.id and j.status != :processing)
             |> lock("FOR UPDATE SKIP LOCKED")
           )
           |> Ecto.Multi.run(:job, fn _repo, %{lock: job} ->
             job =
               if job do
                 job = job |> Job.change(status: :processing) |> @repo.update!
                 handler = job.handler |> :erlang.binary_to_term() |> validate_handler!

                 case apply_handler(handler, job) do
                   {:error, error} ->
                     update_job_as_failed(job, error)

                   {:ok, context} ->
                     update_job_as_success!(job, context)

                   :ok ->
                     update_job_as_success!(job, job.context)
                 end
               else
                 IO.inspect("SKIPPING -- LOCKED")
                 :locked
               end

             {:ok, job}
           end)
           |> @repo.transaction() do
      {:ok, job}
    end
  end

  defp update_job_as_failed(job, error) do
    {1, [job]} =
      Job
      |> Job.update_as_failed(job, error)
      |> @repo.update_all([])

    job
  end

  defp update_job_as_success!(job, context) do
    now = DateTime.utc_now()

    job
    |> Job.change(
      last_succeeded_at: now,
      run_at: now |> DateTime.add(job.interval) |> DateTime.truncate(:second),
      context: context,
      retry_count: 0,
      status: :succeeded
    )
    |> @repo.update!
  end

  defp validate_handler!(handler) when is_atom(handler) do
    if function_exported?(handler, :handle_job, 1) do
      handler
    else
      raise "handler #{inspect(handler)} not a module or handle_job is not defined"
    end
  end

  defp validate_handler!({module, fun}) when is_atom(module) and is_atom(fun) do
    if function_exported?(module, fun, 1) do
      {module, fun}
    else
      raise "handler #{inspect(module)} not a module or #{inspect(fun)} is not defined"
    end
  end

  defp apply_handler({module, fun}, job) do
    apply(module, fun, [job])
  end

  defp apply_handler(handler, job) do
    apply(handler, :handle_job, [job])
  end
end
