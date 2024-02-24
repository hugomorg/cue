defmodule Cue.TestRepo.Migrations.CreateJobsTable do
  use Ecto.Migration

  def change do
    create table :jobs do
      add :name, :string, null: false
      add :handler, :binary, null: false
      add :error_handler, :binary, null: false
      add :retry_count, :integer, default: 0, null: false
      add :run_at, :utc_datetime, null: false
      add :schedule, :string
      add :last_error, :text
      add :context, :binary
      add :last_succeeded_at, :utc_datetime_usec
      add :last_failed_at, :utc_datetime_usec
      add :status, :smallint, null: false
      add :max_retries, :int

      timestamps(updated_at: false)
    end

    create unique_index(:jobs, :name)
    create index(:jobs, :run_at)
  end
end
