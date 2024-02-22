defmodule Cue.Schemas.Job do
  use Ecto.Schema
  import Ecto.Changeset

  @timestamps_opts [type: :utc_datetime_usec]
  @status_values [not_started: 0, processing: 1, failed: 2, succeeded: 3, paused: 4]

  schema "jobs" do
    field(:name, :string)
    field(:handler, Cue.ElixirTerm)
    field(:error_handler, Cue.ElixirTerm)
    field(:last_error, :string)
    field(:schedule, :string)
    field(:retry_count, :integer)
    field(:max_retries, :integer)
    field(:context, Cue.ElixirTerm)
    field(:last_succeeded_at, :utc_datetime_usec)
    field(:last_failed_at, :utc_datetime_usec)
    field(:run_at, :utc_datetime)
    field(:status, Ecto.Enum, values: @status_values)
    field(:one_off, :boolean)

    timestamps(updated_at: false)
  end

  def changeset(job, params) do
    job
    |> cast(params, [
      :status,
      :last_succeeded_at,
      :last_failed_at,
      :last_error,
      :run_at,
      :retry_count,
      :context,
      :max_retries,
      :one_off
    ])
    |> validate_required([:status, :run_at])
    |> validate_number(:max_retries, greater_than_or_equal_to: 0)
    |> validate_number(:retry_count, greater_than_or_equal_to: 0)
  end

  def next_run_at!(%__MODULE__{schedule: schedule}) do
    next_run_at!(schedule)
  end

  def next_run_at!(schedule) when is_binary(schedule) do
    schedule |> Cron.new!() |> Cron.next() |> DateTime.from_naive!("Etc/UTC")
  end

  def retries_exceeded?(%__MODULE__{max_retries: nil}), do: false

  def retries_exceeded?(%__MODULE__{max_retries: max_retries, retry_count: retry_count}) do
    retry_count >= max_retries
  end
end
