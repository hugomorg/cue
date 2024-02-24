defmodule Cue do
  @moduledoc """
  Documentation for `Cue`.
  """

  @type name :: String.t()
  @type state :: any()
  @callback init(name) :: {:ok, any()}
  @callback handle_job(name, state) :: :ok | {:ok, map()} | {:error, any()}
  @type error_info :: %{
          error: String.t(),
          retry_count: non_neg_integer(),
          max_retries: non_neg_integer()
        }
  @callback handle_job_error(name, state, error_info) :: :ok | {:ok, map()} | {:error, any()}

  @optional_callbacks [init: 1]

  alias Cue.Job

  def enqueue!(opts) do
    opts =
      Keyword.validate!(opts, [
        :handler,
        :repo,
        :name,
        :schedule,
        max_retries: nil,
        state: nil
      ])

    handler = validate_job_handler!(opts[:handler])

    state = init_job(opts[:name], handler)

    {schedule, run_at} = validate_schedule!(opts[:schedule])

    %Job{}
    |> Job.changeset(%{
      name: opts[:name],
      handler: handler,
      run_at: run_at,
      schedule: schedule,
      status: :not_started,
      max_retries: opts[:max_retries],
      state: state
    })
    |> opts[:repo].insert!()

    opts[:name]
  end

  def dequeue(repo, job_name) do
    require Ecto.Query

    # Synchronously remove job from scheduling/processing
    Cue.Scheduler.add_job_to_ignored(job_name)
    Cue.Processor.remove_job(job_name)

    {count, _returned} =
      Job
      |> Ecto.Query.where(name: ^job_name)
      |> repo.delete_all()

    Cue.Scheduler.remove_job_from_ignored(job_name)

    count
  end

  defp validate_job_handler!(handler) when is_atom(handler) do
    module_exists? = Code.ensure_loaded?(handler)
    job_handler_defined? = function_exported?(handler, :handle_job, 2)
    error_handler_defined? = function_exported?(handler, :handle_job_error, 3)

    cond do
      module_exists? and job_handler_defined? and error_handler_defined? ->
        handler

      module_exists? and job_handler_defined? ->
        raise "Please define `handle_job_error/3` in #{inspect(handler)}"

      :else ->
        raise "Please define `handle_job/2` in #{inspect(handler)}"
    end
  end

  defp init_job(name, module) do
    if function_exported?(module, :init, 1) do
      case module.init(name) do
        {:ok, state} ->
          state

        unexpected_return ->
          raise "You must return `{:ok, state}` from your `init/1` function, received #{inspect(unexpected_return)}"
      end
    end
  end

  defp validate_schedule!(schedule) when is_binary(schedule) do
    {schedule, Job.next_run_at!(schedule)}
  end

  defp validate_schedule!(%DateTime{} = schedule) do
    {nil, schedule}
  end

  defp validate_schedule!(schedule) do
    raise "schedule must be a valid cron specification or a UTC datetime, received #{inspect(schedule)}"
  end

  defmacro __using__(opts) do
    name = Keyword.get(opts, :name)
    schedule = Keyword.fetch!(opts, :schedule)

    quote do
      @behaviour Cue
      @repo Application.compile_env!(:cue, :repo)
      @cue_name unquote(name) || String.replace("#{__MODULE__}", ~r/^Elixir\./, "")

      def enqueue!(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo
        ]
        |> Keyword.merge(opts)
        |> Cue.enqueue!()
      end

      def dequeue(name \\ @cue_name) do
        Cue.dequeue(@repo, name)
      end
    end
  end
end
