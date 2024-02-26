defmodule Cue.Processor do
  @moduledoc false

  require Logger
  import Ecto.Query
  alias Cue.Job
  @repo Application.compile_env!(:cue, :repo)

  def process_jobs(jobs) do
    Cue.TaskProcessor
    |> Task.Supervisor.async_stream_nolink(jobs, &handle_job/1,
      max_concurrency: 10,
      timeout: 10_000,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    )
    |> Enum.filter(&match?({:exit, _}, &1))
    |> handle_crashed_jobs
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

  defp handle_crashed_jobs(failed_jobs) do
    Enum.each(failed_jobs, fn {:exit, {job, error}} ->
      Logger.warning("job=#{job.id} crashed, error=#{inspect(error)}")
      maybe_apply_error_handler(job)
      update_job_as_failed!(job, error)
    end)
  end
end
