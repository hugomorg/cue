defmodule Cue.Processor do
  @moduledoc false

  alias Cue.Job

  @callback process_jobs([Job.t()]) :: :ok
  @implementation Application.compile_env!(:cue, :processor)

  def impl, do: @implementation
end
