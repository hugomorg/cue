defmodule Cue.TestRepo.Migrations.CreateJobsTable do
  use Ecto.Migration

  def change do
    create table :jobs do
      add :name, :string, null: false
      add :handler, :binary, null: false
      add :last_error, :text
      add :retry_count, :integer, default: 0, null: false
      add :context, :map
      add :last_succeeded_at, :utc_datetime_usec
      add :last_failed_at, :utc_datetime_usec

      timestamps(updated_at: false)
    end
  end
end
