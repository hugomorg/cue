defmodule Cue do
  @moduledoc """
  Documentation for `Cue`.
  """

  @callback handle_job(any()) :: :ok | {:ok, map()} | {:error, any()}
  @callback handle_job_error(any()) :: :ok | {:ok, map()} | {:error, any()}

  def enqueue!(handler, opts) do
    opts =
      Keyword.validate!(opts, [
        :repo,
        :name,
        :schedule,
        :error_handler,
        run_now: false,
        max_retries: nil
      ])

    run_at =
      if opts[:run_now] do
        DateTime.utc_now()
      else
        Cue.Schemas.Job.next_run_at(opts[:schedule])
      end

    opts[:repo].insert!(
      %Cue.Schemas.Job{
        name: opts[:name],
        handler: handler,
        error_handler: opts[:error_handler],
        run_at: run_at,
        schedule: opts[:schedule],
        status: :not_started,
        max_retries: opts[:max_retries]
      },
      on_conflict: :nothing,
      conflict_target: :name,
      returning: true
    )
  end

  def dequeue(repo, name) do
    require Ecto.Query

    {count, _returned} =
      Cue.Schemas.Job
      |> Ecto.Query.where(name: ^name)
      |> repo.delete_all()

    count
  end

  defmacro __using__(opts) do
    name = Keyword.get(opts, :name)
    schedule = Keyword.fetch!(opts, :schedule)
    run_now = Keyword.get(opts, :run_now)
    max_retries = Keyword.get(opts, :max_retries)

    quote do
      @behaviour Cue
      @repo Application.compile_env!(:cue, :repo)
      @cue_name unquote(name) || String.replace("#{__MODULE__}", ~r/^Elixir\./, "")

      def enqueue! do
        Cue.enqueue!(__MODULE__,
          name: @cue_name,
          error_handler: __MODULE__,
          schedule: unquote(schedule),
          repo: @repo,
          run_now: unquote(run_now),
          max_retries: unquote(max_retries)
        )
      end

      def dequeue do
        Cue.dequeue(@repo, @cue_name)
      end
    end
  end
end
