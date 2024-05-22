defmodule Cue.Processor.Impl do
  @moduledoc """
  Responsible for handling the scheduled jobs.
  """

  require Logger
  import Ecto.Query
  alias Cue.Job
  alias Cue.Processor
  @repo Application.compile_env(:cue, :repo)
  @max_concurrency Application.compile_env(:cue, :max_concurrency, 5)
  @timeout Application.compile_env(:cue, :timeout, 5000)

  @behaviour Processor

  @impl true
  def process_jobs(jobs) do
    Cue.TaskProcessor
    |> Task.Supervisor.async_stream_nolink(
      jobs,
      fn job ->
        try do
          handle_job(job)
        rescue
          Ecto.StaleEntryError ->
            Logger.warning(stale_job_message(job))
            {:error, {:stale, job}}
        end
      end,
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

    query =
      where(
        Job,
        [j],
        j.id == ^job.id and j.status != :processing and j.status != :paused
      )

    if job = @repo.one(query) do
      maybe_handle_job(job)
    else
      {:error, :job_not_available}
    end
  end

  defp maybe_handle_job(job = %Job{status: :paused}) do
    job
  end

  defp maybe_handle_job(job = %Job{retry_count: r, max_retries: m})
       when is_integer(r) and is_integer(m) and r >= m do
    job
  end

  defp maybe_handle_job(job) do
    previous_status = job.status

    job = job |> Job.changeset(%{status: :processing}) |> @repo.update!

    case apply(job.handler, :handle_job, [job.name, job.state]) do
      {:error, error} ->
        failed_job = update_job_as_failed!(job, job.state, error, previous_status)
        maybe_apply_error_handler(failed_job)
        failed_job

      {:error, error, {:state, state}} ->
        failed_job = update_job_as_failed!(job, state, error, previous_status)
        maybe_apply_error_handler(failed_job)
        failed_job

      {:ok, {:state, state}} ->
        update_job_as_success!(job, state)

      _ ->
        update_job_as_success!(job, job.state)
    end
  end

  defp update_job_as_failed!(job, state, error, previous_status) do
    now = DateTime.utc_now()

    {status, retry_count} =
      cond do
        Job.one_off?(job) -> {:failed, 0}
        previous_status != :failed -> {:failed, 0}
        job.retry_count + 1 == job.max_retries -> {:paused, job.retry_count + 1}
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

      try do
        refreshed_job = @repo.reload(job)
        failed_job = update_job_as_failed!(refreshed_job, job.state, error, job.status)
        maybe_apply_error_handler(failed_job)
      rescue
        Ecto.StaleEntryError -> Logger.warning(stale_job_message(job))
      end
    end)
  end

  defp stale_job_message(%Job{id: id}) do
    "job=#{id} not updated because of a concurrent update"
  end
end
