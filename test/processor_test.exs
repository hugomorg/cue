defmodule Cue.ProcessorTest do
  use Cue.DataCase
  doctest Cue
  @repo Cue.TestRepo
  alias Cue.Job

  import Hammox
  use Hammox.Protect, module: Cue.Processor.Impl, behaviour: Cue.Processor

  setup :verify_on_exit!

  setup do
    start_supervised!({Task.Supervisor, name: Cue.TaskProcessor})

    agent =
      start_supervised!(
        {Agent, fn -> Agent.start_link(fn -> %{success: nil, error: nil} end, name: :agent) end}
      )

    %{agent: agent}
  end

  # Key under the unique test name to avoid collisions
  defmodule Example do
    use Cue, schedule: "* * * * * *", max_retries: 3

    def handle_job(name, state = %{test: test}) do
      if state[:fun], do: state[:fun].()

      Agent.update(:agent, &Map.put(&1, test, {name, state}))

      if state[:error] do
        {:error, "foo"}
      else
        :ok
      end
    end

    def handle_job_error(name, state, error) do
      Agent.update(:agent, &Map.put(&1, :error, {name, error, state}))

      if state[:error_fun], do: state[:error_fun].()

      :ok
    end
  end

  describe "processes jobs" do
    test "calls handler, sets fields on job when done", context do
      now = DateTime.utc_now()

      state = %{test: context.test}
      job = make_job!(state: state)
      assert process_jobs([job]) == :ok

      job = @repo.reload(job)
      assert job.status == :succeeded
      assert DateTime.compare(now, job.last_succeeded_at) == :lt

      assert Agent.get(:agent, &Map.fetch!(&1, context.test)) == {job.name, state}
    end

    test "avoids race conditions", context do
      now = DateTime.utc_now()

      state = %{
        test: context.test,
        fun: fn ->
          Agent.update(:agent, fn state ->
            Map.update(state, {context.test, :counter}, 1, &(&1 + 1))
          end)
        end
      }

      job = make_job!(state: state)
      assert process_jobs([job, job, job]) == :ok

      job = @repo.reload(job)
      assert job.status == :succeeded
      assert DateTime.compare(now, job.last_succeeded_at) == :lt

      assert Agent.get(:agent, &Map.fetch!(&1, {context.test, :counter})) == 1
    end

    test "if max retries set stop after that many failures - crashes", context do
      now = DateTime.utc_now()

      state = %{
        test: context.test,
        fun: fn ->
          Agent.update(:agent, fn state ->
            Map.update(state, {context.test, :counter}, 1, &(&1 + 1))
          end)

          raise "foo"
        end
      }

      job = make_job!(state: state, max_retries: 3)
      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert DateTime.compare(now, job.last_failed_at) == :lt
      assert job.last_error =~ "foo"
      assert job.retry_count == 0

      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert job.retry_count == 1

      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert job.retry_count == 2

      # Shouldn't run on 4th time
      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :paused
      assert job.retry_count == 3

      assert Agent.get(:agent, &Map.fetch!(&1, {context.test, :counter})) == 4
    end

    test "if max retries set stop after that many failures - errors", context do
      now = DateTime.utc_now()

      state = %{
        error: true,
        test: context.test,
        fun: fn ->
          Agent.update(:agent, fn state ->
            Map.update(state, {context.test, :counter}, 1, &(&1 + 1))
          end)
        end,
        error_fun: fn ->
          Agent.update(:agent, fn state ->
            Map.update(state, {context.test, :error_counter}, 1, &(&1 + 1))
          end)
        end
      }

      job = make_job!(state: state, max_retries: 3)
      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert DateTime.compare(now, job.last_failed_at) == :lt
      assert job.last_error =~ "foo"
      assert job.retry_count == 0

      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert job.retry_count == 1

      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert job.retry_count == 2

      # Shouldn't run on 4th time
      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :paused
      assert job.retry_count == 3

      assert Agent.get(:agent, &Map.fetch!(&1, {context.test, :counter})) == 4
      assert Agent.get(:agent, &Map.fetch!(&1, {context.test, :error_counter})) == 3
    end

    test "one-off jobs not retried", context do
      now = DateTime.utc_now()

      state = %{
        error: true,
        test: context.test,
        fun: fn ->
          Agent.update(:agent, fn state ->
            Map.update(state, {context.test, :counter}, 1, &(&1 + 1))
          end)
        end
      }

      job = make_job!(state: state, schedule: nil, max_retries: 3)

      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert DateTime.compare(now, job.last_failed_at) == :lt
      assert job.last_error =~ "foo"
      assert job.retry_count == 0

      # This second one won't get scheduled - just to test
      assert process_jobs([job]) == :ok
      job = @repo.reload(job)
      assert job.status == :failed
      assert job.retry_count == 0

      assert Agent.get(:agent, &Map.fetch!(&1, {context.test, :counter})) == 2
    end
  end

  defp make_job!(opts \\ [])

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
          status: :not_started,
          run_at: DateTime.utc_now()
        },
        opts
      )

    %Job{}
    |> Job.changeset(params)
    |> @repo.insert!()
  end

  defp unique_id, do: System.unique_integer([:positive, :monotonic])
end
