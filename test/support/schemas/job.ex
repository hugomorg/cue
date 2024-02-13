defmodule Cue.Schemas.Job do
  use Ecto.Schema

  @timestamps_opts [type: :utc_datetime_usec]

  schema "jobs" do
    field(:name, :string)
    field(:handler, :binary)
    field(:last_error, :string)
    field(:retry_count, :integer)
    field(:context, :map)
    field(:last_succeeded_at, :utc_datetime_usec)
    field(:last_failed_at, :utc_datetime_usec)
    field(:run_at, :utc_datetime)
    field(:interval, :integer)

    timestamps(updated_at: false)
  end
end
