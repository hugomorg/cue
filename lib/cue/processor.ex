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
    {:ok, %{jobs: %{}}}
  end

  # Processing logic
  def process_job(job) do
    GenServer.call(__MODULE__, {:start_task, job})
  end

  @impl true
  # In this case the task is already running, so we just return :ok.
  def handle_call({:start_task, job}, _from, %{jobs: jobs} = state)
      when is_map_key(jobs, job.id) do
    Logger.debug("already handling job=#{job.id}")

    {:reply, :ok, state}
  end

  @impl true
  # The task is not running yet, so let's start it.
  def handle_call({:start_task, job}, _from, %{jobs: jobs} = state) do
    Logger.debug("handling job=#{job.id}")

    _task =
      Task.Supervisor.async_nolink(Cue.TaskProcessor, fn ->
        handle_job(job)
      end)

    # We return :ok and the server will continue running
    {:reply, :ok, %{state | jobs: Map.put(jobs, job.id, job)}}
  end

  # def handle_call(call, from, state) do
  #   IO.inspect("call=#{inspect(call)}")
  #   {:reply, :ok, state}
  # end

  @impl true
  # Skipping because of lock
  def handle_info({ref, {:ok, :locked}}, state) do
    Logger.debug("skipping - locked")
    # We don't care about the DOWN message now, so let's demonitor and flush it
    Process.demonitor(ref, [:flush])
    {:noreply, state}
  end

  @impl true
  # The task completed successfully
  def handle_info({ref, {:ok, job}}, %{jobs: jobs} = state) do
    Logger.debug("finished ok result=#{inspect(job)}")
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | jobs: Map.delete(jobs, job.id)}}
  end

  # The task failed
  def handle_info({:DOWN, _ref, :process, _pid, reason}, %{jobs: jobs} = state) do
    Logger.debug("error reason=#{inspect(reason)}")
    # Log and possibly restart the task...
    {:noreply, %{state | jobs: jobs}}
  end

  def handle_job(job) do
    Logger.debug("handling job=#{inspect(job)}")

    with {:ok, %{job: job}} <-
           Ecto.Multi.new()
           |> Ecto.Multi.run(:job, fn _repo, _changes ->
             job =
               Job
               |> where([j], j.id == ^job.id and j.status != :processing)
               |> lock("FOR UPDATE SKIP LOCKED")
               |> @repo.one

             job =
               if job do
                 job |> Job.change(status: :processing) |> @repo.update
                 handler = :erlang.binary_to_term(job.handler)

                 if function_exported?(handler, :handle_job, 1) do
                   try do
                     case handler.handle_job(job) do
                       {:error, error} ->
                         update_job_as_failed(job, error)

                       {:ok, context} ->
                         update_job_as_success!(job, context)

                       :ok ->
                         update_job_as_success!(job, job.context)
                     end
                   rescue
                     error ->
                       Logger.error("handler crashed: #{inspect(error)}")
                       update_job_as_failed(job, error)
                   end
                 else
                   raise "handler #{inspect(handler)} not found or does not implement handle_job/1"
                 end
               else
                 IO.inspect("skipping, locked")
                 :locked
               end

             {:ok, job}
           end)
           |> @repo.transaction() do
      {:ok, job}
    end
  end

  defp update_job_as_failed(job, error) do
    {1, job} =
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
end
