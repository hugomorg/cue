defmodule Cue.Scheduler do
  @moduledoc false

  use GenServer
  require Logger
  import Ecto.Query
  alias Cue.Schemas.Job
  @repo Cue.TestRepo

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
    jobs = Job |> where([j], ^DateTime.utc_now() >= j.run_at) |> @repo.all()

    for job <- jobs do
      Ecto.Multi.new()
      |> Ecto.Multi.run(:lock, fn _repo, _changes ->
        job =
          Job
          |> where([j], j.id == ^job.id and j.status != :processing)
          |> lock("FOR UPDATE SKIP LOCKED")
          |> @repo.one

        if job do
          Job |> where(id: ^job.id) |> @repo.update_all(set: [status: :processing])
          handler = :erlang.binary_to_term(job.handler)

          if function_exported?(handler, :handle_job, 1) do
            try do
              case handler.handle_job(job) do
                {:error, reason} ->
                  now = DateTime.utc_now()

                  Job
                  |> where(id: ^job.id)
                  |> @repo.update_all(
                    set: [
                      last_failed_at: now,
                      run_at: DateTime.add(now, job.interval),
                      last_error: inspect(reason),
                      status: :failed
                    ],
                    inc: [retry_count: 1]
                  )

                {:ok, context} ->
                  now = DateTime.utc_now()

                  Job
                  |> where(id: ^job.id)
                  |> @repo.update_all(
                    set: [
                      last_succeeded_at: now,
                      run_at: DateTime.add(now, job.interval),
                      context: context,
                      retry_count: 0,
                      status: :succeeded
                    ]
                  )

                :ok ->
                  now = DateTime.utc_now()

                  Job
                  |> where(id: ^job.id)
                  |> @repo.update_all(
                    set: [
                      last_succeeded_at: now,
                      run_at: DateTime.add(now, job.interval),
                      retry_count: 0,
                      status: :succeeded
                    ]
                  )
              end
            rescue
              error ->
                Logger.error("handler crashed: #{inspect(error)}")
                now = DateTime.utc_now()

                Job
                |> where(id: ^job.id)
                |> @repo.update_all(
                  set: [
                    last_failed_at: now,
                    run_at: DateTime.add(now, job.interval),
                    last_error: inspect(error),
                    status: :failed
                  ],
                  inc: [retry_count: 1]
                )
            end
          else
            Logger.error(
              "handler #{inspect(handler)} not found or does not implement handle_job/1"
            )
          end
        else
          IO.inspect("skipping, locked")
        end

        {:ok, :ok}
      end)
      |> @repo.transaction()
    end

    {:noreply, state}
  end

  defp loop(task, seconds) do
    Process.send_after(self(), task, :timer.seconds(seconds))
  end
end
