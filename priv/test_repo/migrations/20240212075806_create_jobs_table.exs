defmodule Cue.TestRepo.Migrations.CreateJobsTable do
  use Ecto.Migration

  def change do
    create table :jobs do
      add :name, :string, null: false
      add :handler, :binary, null: false
      add :retry_count, :integer, default: 0, null: false
      add :run_at, :utc_datetime, null: false
      add :last_error, :text
      add :context, :map
      add :last_succeeded_at, :utc_datetime_usec
      add :last_failed_at, :utc_datetime_usec
      add :interval, :integer

      timestamps(updated_at: false)
    end

    create unique_index(:jobs, :name)
    create index(:jobs, :run_at)
  end
end
