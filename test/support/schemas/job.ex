defmodule Cue.Schemas.Job do
  use Ecto.Schema
  import Ecto.Query

  @timestamps_opts [type: :utc_datetime_usec]
  @status_values [not_started: 0, processing: 1, failed: 2, succeeded: 3]

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
    field(:status, Ecto.Enum, values: @status_values)

    timestamps(updated_at: false)
  end

  def change(job, updates \\ %{}) do
    Ecto.Changeset.change(job, updates)
  end

  def increment_retry_count(query \\ __MODULE__) do
    update(query, inc: [retry_count: 1])
  end

  def update_run_at(query \\ __MODULE__, job) do
    run_at = DateTime.utc_now() |> DateTime.add(job.interval) |> DateTime.truncate(:second)
    update(query, set: [run_at: ^run_at])
  end

  def update_status(query \\ __MODULE__, status) do
    update(query, set: [status: ^status])
  end

  def update_last_error(query \\ __MODULE__, error) do
    update(query, set: [last_error: ^inspect(error)])
  end

  def update_as_failed(query \\ __MODULE__, job, error) do
    query
    |> where(id: ^job.id)
    |> select([j], j)
    |> update_status(:failed)
    |> update_last_error(error)
    |> update_run_at(job)
    |> increment_retry_count()
    |> update(set: [last_failed_at: ^DateTime.utc_now()])
  end

  def update_as_succeeded(query \\ __MODULE__, job) do
    query
    |> where(id: ^job.id)
    |> select([j], j)
    |> update_status(:succeeded)
    |> update_run_at(job)
    |> update(set: [last_succeeded_at: ^DateTime.utc_now()])
  end
end
