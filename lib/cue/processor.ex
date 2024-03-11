defmodule Cue.Processor do
  @moduledoc false

  require Logger
  import Ecto.Query
  alias Cue.Job
  @repo Application.compile_env(:cue, :repo)
  @max_concurrency Application.compile_env(:cue, :max_concurrency, 5)
  @timeout Application.compile_env(:cue, :timeout, 5000)

  def process_jobs(jobs) do
    Cue.TaskProcessor
    |> Task.Supervisor.async_stream_nolink(jobs, &handle_job/1,
      max_concurrency: @max_concurrency,
      timeout: @timeout,
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
           |> Ecto.Multi.run(:maybe_remove_job, fn _repo, changes ->
             maybe_remove_job(changes.job)
           end)
           |> @repo.transaction() do
      {:ok, job}
    end
  end

  defp maybe_handle_job(%{free_job: nil}), do: {:ok, :locked}

  defp maybe_handle_job(changes) do
    previous_status = changes.free_job.status
    job = changes.free_job |> Job.changeset(%{status: :processing}) |> @repo.update!

    job =
      case apply(job.handler, :handle_job, [job.name, job.state]) do
        {:error, error} ->
          failed_job = update_job_as_failed!(job, job.state, error, previous_status)
          maybe_apply_error_handler(failed_job)
          failed_job

        {:error, error, state} ->
          failed_job = update_job_as_failed!(job, state, error, previous_status)
          maybe_apply_error_handler(failed_job)
          failed_job

        {:ok, state} ->
          update_job_as_success!(job, state)

        :ok ->
          update_job_as_success!(job, job.state)
      end

    {:ok, job}
  end

  defp maybe_remove_job(job = %Job{}) do
    {:ok, Job.remove?(job) and !!@repo.delete!(job)}
  end

  defp maybe_remove_job(:locked) do
    {:ok, :locked}
  end

  defp update_job_as_failed!(job, state, error, previous_status) do
    now = DateTime.utc_now()

    {status, retry_count} =
      cond do
        previous_status not in [:paused, :failed] -> {:failed, 0}
        Job.one_off?(job) -> {:failed, 0}
        Job.retries_exceeded?(job) -> {:paused, job.retry_count}
        :else -> {:failed, job.retry_count + 1}
      end

    last_error =
      if is_binary(error) do
        error
      else
        inspect(error)
      end

    job
    |> Job.changeset(%{
      last_failed_at: now,
      last_error: last_error,
      run_at: Job.next_run_at!(job),
      retry_count: retry_count,
      status: status,
      state: state
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

  defp maybe_apply_error_handler(%Job{status: :paused}), do: :skip

  defp maybe_apply_error_handler(job) do
    apply(job.handler, :handle_job_error, [
      job.name,
      job.state,
      %{error: job.last_error, retry_count: job.retry_count, max_retries: job.max_retries}
    ])
  end

  defp handle_crashed_jobs(failed_jobs) do
    # `job` in error callback will be job passed into stream function
    # that is, the job whose status has not been marked as `processing`
    Enum.each(failed_jobs, fn {:exit, {job, error}} ->
      Logger.warning("job=#{job.id} crashed, error=#{inspect(error)}")
      failed_job = update_job_as_failed!(job, job.state, error, job.status)
      maybe_apply_error_handler(failed_job)
    end)
  end
end
