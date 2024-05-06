defmodule CueTest do
  use Cue.DataCase
  doctest Cue
  @repo Cue.TestRepo
  alias Cue.Job

  defmodule NoCallbacks do
    use Cue
  end

  defmodule NoHandleJobCallback do
    use Cue

    def handle_job_error(_, _, _), do: :ok
  end

  defmodule NoHandleJobErrorCallback do
    use Cue

    def handle_job(_, _), do: :ok
  end

  defmodule Example do
    use Cue

    def handle_job(_, _), do: :ok

    def handle_job_error(_, _, _), do: :ok
  end

  describe "create_job/1" do
    test "validates" do
      assert {:error, {:invalid_schedule, _msg}} = Cue.create_job([])
      assert {:error, {:invalid_handler, _msg}} = Cue.create_job(schedule: DateTime.utc_now())

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job(schedule: DateTime.utc_now(), handler: NoCallbacks)

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job(schedule: DateTime.utc_now(), handler: NoHandleJobCallback)

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job(schedule: DateTime.utc_now(), handler: NoHandleJobErrorCallback)

      assert {:error, :no_repo} = Cue.create_job(schedule: DateTime.utc_now(), handler: Example)

      assert {:error, :no_name} =
               Cue.create_job(schedule: DateTime.utc_now(), handler: Example, repo: @repo)

      now = DateTime.utc_now()

      assert {:ok, job} =
               Cue.create_job(
                 schedule: now,
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      assert job.name == "job"
      assert job.run_at == DateTime.truncate(now, :second)
    end
  end

  describe "create_job!/1" do
    test "validates" do
      assert_raise Cue.Error, fn ->
        Cue.create_job!([])
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now())
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now(), handler: NoCallbacks)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now(), handler: NoHandleJobCallback)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now(), handler: NoHandleJobErrorCallback)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now(), handler: Example)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now(), handler: Example, repo: @repo)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job!(schedule: DateTime.utc_now(), handler: Example, repo: @repo)
      end

      now = DateTime.utc_now()

      assert job =
               Cue.create_job!(
                 schedule: now,
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      assert job.name == "job"
      assert job.run_at == DateTime.truncate(now, :second)
    end
  end
end
