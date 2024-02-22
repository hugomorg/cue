defmodule Cue do
  @moduledoc """
  Documentation for `Cue`.
  """

  @type name :: String.t()
  @type context :: any()
  @callback handle_job(name, context) :: :ok | {:ok, map()} | {:error, any()}
  @type error_info :: %{
          error: String.t(),
          retry_count: non_neg_integer(),
          max_retries: non_neg_integer()
        }
  @callback handle_job_error(name, context, error_info) :: :ok | {:ok, map()} | {:error, any()}

  def enqueue!(opts) do
    opts =
      Keyword.validate!(opts, [
        :handler,
        :repo,
        :name,
        :schedule,
        :error_handler,
        run_now: false,
        one_off: false,
        max_retries: nil,
        context: nil
      ])

    run_at =
      if opts[:run_now] do
        DateTime.utc_now()
      else
        Cue.Schemas.Job.next_run_at!(opts[:schedule])
      end

    opts[:repo].insert!(
      %Cue.Schemas.Job{
        name: opts[:name],
        handler: opts[:handler],
        error_handler: opts[:error_handler],
        run_at: run_at,
        schedule: opts[:schedule],
        status: :not_started,
        max_retries: opts[:max_retries],
        one_off: opts[:one_off]
      },
      on_conflict: :nothing,
      conflict_target: :name,
      returning: true
    )
  end

  def dequeue(repo, job_name) do
    require Ecto.Query

    # Synchronously remove job from scheduling/processing
    Cue.Scheduler.add_job_to_ignored(job_name)
    Cue.Processor.remove_job(job_name)

    {count, _returned} =
      Cue.Schemas.Job
      |> Ecto.Query.where(name: ^job_name)
      |> repo.delete_all()

    Cue.Scheduler.remove_job_from_ignored(job_name)

    count
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
          error_handler: __MODULE__,
          schedule: unquote(schedule),
          repo: @repo
        ]
        |> Keyword.merge(opts)
        |> Cue.enqueue!()
      end

      def dequeue do
        Cue.dequeue(@repo, @cue_name)
      end
    end
  end
end
