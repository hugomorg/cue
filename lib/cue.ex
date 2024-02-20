defmodule Cue do
  @moduledoc """
  Documentation for `Cue`.
  """

  @callback handle_job(any()) :: :ok | {:ok, map()} | {:error, any()}
  @callback handle_job_error(any()) :: :ok | {:ok, map()} | {:error, any()}

  defmacro __using__(opts) do
    name = Keyword.get(opts, :name)
    schedule = Keyword.fetch!(opts, :schedule)

    quote do
      @behaviour Cue
      @repo Application.compile_env!(:cue, :repo)
      @cue_name unquote(name) || String.replace("#{__MODULE__}", ~r/^Elixir\./, "")

      def put_on_queue do
        run_at =
          unquote(schedule) |> Cron.new!() |> Cron.next() |> DateTime.from_naive!("Etc/UTC")

        @repo.insert!(
          %Cue.Schemas.Job{
            name: @cue_name,
            handler: __MODULE__,
            error_handler: __MODULE__,
            run_at: run_at,
            schedule: unquote(schedule),
            status: :not_started
          },
          on_conflict: :nothing,
          conflict_target: :name
        )
      end

      def delete_from_queue do
        require Ecto.Query

        Cue.Schemas.Job
        |> Ecto.Query.where(name: @cue_name)
        |> @repo.delete_all()
      end
    end
  end
end
