defmodule Cue.Scheduler do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query

  @check_every_seconds 1

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Keyword.get(args, :name, __MODULE__))
  end

  @impl true
  def init(_args) do
    loop(:check, @check_every_seconds)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:check, state) do
    loop(:check, @check_every_seconds)
    jobs = Cue.Schemas.Job |> where([j], ^DateTime.utc_now() >= j.run_at) |> Cue.TestRepo.all()

    for job <- jobs do
      handler = :erlang.binary_to_term(job.handler)

      if function_exported?(handler, :handle_job, 1) do
        try do
          case handler.handle_job(job) do
            {:error, reason} ->
              now = DateTime.utc_now()

              Cue.Schemas.Job
              |> where(id: ^job.id)
              |> Cue.TestRepo.update_all(
                set: [
                  last_failed_at: now,
                  run_at: DateTime.add(now, job.interval),
                  last_error: inspect(reason)
                ],
                inc: [retry_count: 1]
              )

            {:ok, context} ->
              now = DateTime.utc_now()

              Cue.Schemas.Job
              |> where(id: ^job.id)
              |> Cue.TestRepo.update_all(
                set: [
                  last_succeeded_at: now,
                  run_at: DateTime.add(now, job.interval),
                  context: context,
                  retry_count: 0
                ]
              )

            :ok ->
              now = DateTime.utc_now()

              Cue.Schemas.Job
              |> where(id: ^job.id)
              |> Cue.TestRepo.update_all(
                set: [
                  last_succeeded_at: now,
                  run_at: DateTime.add(now, job.interval),
                  retry_count: 0
                ]
              )
          end
        rescue
          error ->
            Logger.error("handler crashed: #{inspect(error)}")
            now = DateTime.utc_now()

            Cue.Schemas.Job
            |> where(id: ^job.id)
            |> Cue.TestRepo.update_all(
              set: [
                last_failed_at: now,
                run_at: DateTime.add(now, job.interval),
                last_error: inspect(error)
              ],
              inc: [retry_count: 1]
            )
        end
      else
        Logger.error("handler #{inspect(handler)} not found or does not implement handle_job/1")
      end
    end

    {:noreply, state}
  end

  defp loop(task, seconds) do
    Process.send_after(self(), task, :timer.seconds(seconds))
  end
end
