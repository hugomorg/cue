defmodule Cue.Migration do
  defmacro __using__(_) do
    quote do
      use Ecto.Migration

      def change do
        create table(:cue_jobs) do
          add(:name, :string, null: false)
          add(:handler, :binary, null: false)
          add(:retry_count, :integer, default: 0, null: false)
          add(:run_at, :utc_datetime, null: false)
          add(:schedule, :string)
          add(:last_error, :text)
          add(:state, :binary)
          add(:last_succeeded_at, :utc_datetime_usec)
          add(:last_failed_at, :utc_datetime_usec)
          add(:status, :smallint, null: false)
          add(:max_retries, :int)
          add(:autoremove, :boolean, default: false, null: false)

          timestamps(updated_at: false)
        end

        create(unique_index(:cue_jobs, :name))
        create(index(:cue_jobs, :run_at))
      end
    end
  end
end
