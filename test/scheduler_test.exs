defmodule Cue.SchedulerTest do
  use Cue.DataCase
  doctest Cue
  @repo Cue.TestRepo
  alias Cue.Job

  import Hammox

  setup :set_mox_from_context

  defmodule Example do
    use Cue, schedule: "* * * * * *", max_retries: 3
    def handle_job(_, _), do: :ok

    def handle_job_error(_, _, _), do: :ok
  end

  describe "schedules jobs" do
    test "checks jobs and calls processor" do
      parent = self()
      ref = make_ref()

      now = DateTime.utc_now()

      now_plus_one = DateTime.add(now, 1, :millisecond)
      one_off_job = make_job!(handler: Example, run_at: now, status: :not_started, schedule: nil)

      _one_off_job_failed =
        make_job!(handler: Example, run_at: now, status: :failed, schedule: nil)

      _too_many_retries =
        make_job!(handler: Example, run_at: now, max_retries: 3, retry_count: 4, status: :failed)

      no_max_retries =
        make_job!(
          handler: Example,
          run_at: now_plus_one,
          max_retries: nil,
          retry_count: 4,
          status: :failed
        )

      expect(Cue.Processor.Mock, :process_jobs, fn jobs ->
        assert Enum.map(jobs, & &1.id) == Enum.map([one_off_job, no_max_retries], & &1.id)
        send(parent, {ref, :process_jobs})

        :ok
      end)

      start_supervised!({Cue.Scheduler.Impl, run_once: true})
      assert_receive {^ref, :process_jobs}, 200
      :timer.sleep(200)
      stop_supervised!(Cue.Scheduler.Impl)

      verify!()
    end
  end

  defp make_job!(opts) when is_list(opts) do
    opts |> Map.new() |> make_job!()
  end

  defp make_job!(opts) do
    params =
      Map.merge(
        %{
          schedule: "* * * * *",
          handler: Example,
          repo: @repo,
          name: "job-#{unique_id()}",
          status: :not_started
        },
        opts
      )

    %Job{}
    |> Job.changeset(params)
    |> @repo.insert!()
  end

  defp unique_id, do: System.unique_integer([:positive, :monotonic])
end
