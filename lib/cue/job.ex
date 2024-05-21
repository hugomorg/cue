defmodule Cue.Job do
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @timestamps_opts [type: :utc_datetime_usec]
  @status_values [not_started: 0, processing: 1, failed: 2, succeeded: 3, paused: 4]

  schema "cue_jobs" do
    field(:handler, Cue.ElixirTerm)
    field(:last_error, :string)
    field(:last_failed_at, :utc_datetime_usec)
    field(:last_succeeded_at, :utc_datetime_usec)
    field(:max_retries, :integer)
    field(:name, :string)
    field(:retry_count, :integer)
    field(:run_at, :utc_datetime)
    field(:schedule, :string)
    field(:state, Cue.ElixirTerm)
    field(:status, Ecto.Enum, values: @status_values)
    field(:lock_version, :integer, default: 1)

    timestamps(updated_at: false)
  end

  def changeset(job, params) do
    job
    |> cast(params, [
      :handler,
      :last_error,
      :last_failed_at,
      :last_succeeded_at,
      :max_retries,
      :name,
      :retry_count,
      :run_at,
      :schedule,
      :state,
      :status
    ])
    |> validate_required([:status, :run_at, :name, :handler])
    |> validate_number(:max_retries, greater_than_or_equal_to: 0)
    |> validate_number(:retry_count, greater_than_or_equal_to: 0)
    |> unique_constraint(:name)
    |> optimistic_lock(:lock_version)
  end

  def next_run_at!(%__MODULE__{schedule: nil, run_at: run_at}) do
    run_at
  end

  def next_run_at!(%__MODULE__{schedule: schedule}) do
    next_run_at!(schedule)
  end

  def next_run_at!(schedule) when is_binary(schedule) do
    schedule |> Cron.new!() |> Cron.next() |> DateTime.from_naive!("Etc/UTC")
  end

  def next_run_at(%__MODULE__{schedule: nil, run_at: run_at}) do
    {:ok, run_at}
  end

  def next_run_at(%__MODULE__{schedule: schedule}) do
    next_run_at(schedule)
  end

  def next_run_at(schedule) when is_binary(schedule) do
    with {:ok, cron} <- Cron.new(schedule) do
      cron |> Cron.next() |> DateTime.from_naive("Etc/UTC")
    end
  end

  def one_off?(%__MODULE__{schedule: nil}), do: true
  def one_off?(%__MODULE__{}), do: false

  def retries_exceeded?(%__MODULE__{max_retries: nil}), do: false

  def retries_exceeded?(%__MODULE__{max_retries: max_retries, retry_count: retry_count}) do
    retry_count >= max_retries
  end
end
