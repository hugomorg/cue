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
end
