defmodule Cue.Schemas.Job do
  use Ecto.Schema
  import Ecto.Changeset

  @timestamps_opts [type: :utc_datetime_usec]
  @status_values [not_started: 0, processing: 1, failed: 2, succeeded: 3]

  schema "jobs" do
    field(:name, :string)
    field(:handler, :binary)
    field(:error_handler, :binary)
    field(:last_error, :string)
    field(:retry_count, :integer)
    field(:context, :map)
    field(:last_succeeded_at, :utc_datetime_usec)
    field(:last_failed_at, :utc_datetime_usec)
    field(:run_at, :utc_datetime)
    field(:interval, :integer)
    field(:status, Ecto.Enum, values: @status_values)

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
      :retry_count
    ])
    |> validate_required([:status, :run_at])
    |> validate_number(:retry_count, greater_than_or_equal_to: 0)
  end
end
