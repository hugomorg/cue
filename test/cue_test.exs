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

    test "passes on necessary options and sets defaults" do
      assert {:ok, job} =
               Cue.create_job(
                 schedule: DateTime.utc_now(),
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      inserted_job = @repo.one!(Job)

      assert inserted_job.handler == Example
      assert inserted_job.name == job.name
      assert inserted_job.retry_count == 0
      assert inserted_job.run_at == job.run_at
      assert inserted_job.status == :not_started

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.schedule
      refute inserted_job.state
    end

    test "other settings can be overridden" do
      assert {:ok, _job} =
               Cue.create_job(
                 schedule: "*/1 * * * *",
                 handler: Example,
                 repo: @repo,
                 name: "job",
                 max_retries: 5,
                 autoremove: true,
                 state: %{key: :value}
               )

      inserted_job = @repo.one!(Job)

      assert inserted_job.autoremove
      assert inserted_job.max_retries == 5
      assert inserted_job.schedule == "*/1 * * * *"
      assert inserted_job.state == %{key: :value}
    end

    test "returns error if job with name exists" do
      params = [
        handler: Example,
        name: "job",
        repo: @repo,
        schedule: "*/1 * * * *"
      ]

      assert {:ok, _job} = Cue.create_job(params)
      assert {:error, {:job_exists, "job"}} = Cue.create_job(params)
    end
  end

  describe "create_job_unless_exists/1" do
    test "validates" do
      assert {:error, {:invalid_schedule, _msg}} = Cue.create_job_unless_exists([])

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job_unless_exists(schedule: DateTime.utc_now())

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job_unless_exists(schedule: DateTime.utc_now(), handler: NoCallbacks)

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job_unless_exists(
                 schedule: DateTime.utc_now(),
                 handler: NoHandleJobCallback
               )

      assert {:error, {:invalid_handler, _msg}} =
               Cue.create_job_unless_exists(
                 schedule: DateTime.utc_now(),
                 handler: NoHandleJobErrorCallback
               )

      assert {:error, :no_repo} =
               Cue.create_job_unless_exists(schedule: DateTime.utc_now(), handler: Example)

      assert {:error, :no_name} =
               Cue.create_job_unless_exists(
                 schedule: DateTime.utc_now(),
                 handler: Example,
                 repo: @repo
               )

      now = DateTime.utc_now()

      assert {:ok, job} =
               Cue.create_job_unless_exists(
                 schedule: now,
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      assert job.name == "job"
      assert job.run_at == DateTime.truncate(now, :second)
    end

    test "passes on necessary options and sets defaults" do
      assert {:ok, job} =
               Cue.create_job_unless_exists(
                 schedule: DateTime.utc_now(),
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      inserted_job = @repo.one!(Job)

      assert inserted_job.handler == Example
      assert inserted_job.name == job.name
      assert inserted_job.retry_count == 0
      assert inserted_job.run_at == job.run_at
      assert inserted_job.status == :not_started

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.schedule
      refute inserted_job.state
    end

    test "other settings can be overridden" do
      assert {:ok, _job} =
               Cue.create_job_unless_exists(
                 schedule: "*/1 * * * *",
                 handler: Example,
                 repo: @repo,
                 name: "job",
                 max_retries: 5,
                 autoremove: true,
                 state: %{key: :value}
               )

      inserted_job = @repo.one!(Job)

      assert inserted_job.autoremove
      assert inserted_job.max_retries == 5
      assert inserted_job.schedule == "*/1 * * * *"
      assert inserted_job.state == %{key: :value}
    end

    test "no error if job with name exists" do
      params = [
        handler: Example,
        name: "job",
        repo: @repo,
        schedule: "*/1 * * * *"
      ]

      assert {:ok, job} = Cue.create_job_unless_exists(params)
      assert {:ok, ^job} = Cue.create_job_unless_exists(params)
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

    test "passes on necessary options and sets defaults" do
      job =
        Cue.create_job!(
          schedule: DateTime.utc_now(),
          handler: Example,
          repo: @repo,
          name: "job"
        )

      inserted_job = @repo.one!(Job)

      assert inserted_job.handler == Example
      assert inserted_job.name == job.name
      assert inserted_job.retry_count == 0
      assert inserted_job.run_at == job.run_at
      assert inserted_job.status == :not_started

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.schedule
      refute inserted_job.state
    end

    test "other settings can be overridden" do
      Cue.create_job!(
        schedule: "*/1 * * * *",
        handler: Example,
        repo: @repo,
        name: "job",
        max_retries: 5,
        autoremove: true,
        state: %{key: :value}
      )

      inserted_job = @repo.one!(Job)

      assert inserted_job.autoremove
      assert inserted_job.max_retries == 5
      assert inserted_job.schedule == "*/1 * * * *"
      assert inserted_job.state == %{key: :value}
    end

    test "raises if job with name exists" do
      params = [
        handler: Example,
        name: "job",
        repo: @repo,
        schedule: DateTime.utc_now()
      ]

      Cue.create_job!(params)

      assert_raise Ecto.InvalidChangesetError, fn ->
        Cue.create_job!(params)
      end
    end
  end

  describe "create_job_unless_exists!/1" do
    test "validates" do
      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!([])
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(schedule: DateTime.utc_now())
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(schedule: DateTime.utc_now(), handler: NoCallbacks)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(schedule: DateTime.utc_now(), handler: NoHandleJobCallback)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(
          schedule: DateTime.utc_now(),
          handler: NoHandleJobErrorCallback
        )
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(schedule: DateTime.utc_now(), handler: Example)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(schedule: DateTime.utc_now(), handler: Example, repo: @repo)
      end

      assert_raise Cue.Error, fn ->
        Cue.create_job_unless_exists!(schedule: DateTime.utc_now(), handler: Example, repo: @repo)
      end

      now = DateTime.utc_now()

      assert job =
               Cue.create_job_unless_exists!(
                 schedule: now,
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      assert job.name == "job"
      assert job.run_at == DateTime.truncate(now, :second)
    end

    test "passes on necessary options and sets defaults" do
      job =
        Cue.create_job_unless_exists!(
          schedule: DateTime.utc_now(),
          handler: Example,
          repo: @repo,
          name: "job"
        )

      inserted_job = @repo.one!(Job)

      assert inserted_job.handler == Example
      assert inserted_job.name == job.name
      assert inserted_job.retry_count == 0
      assert inserted_job.run_at == job.run_at
      assert inserted_job.status == :not_started

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.schedule
      refute inserted_job.state
    end

    test "other settings can be overridden" do
      Cue.create_job_unless_exists!(
        schedule: "*/1 * * * *",
        handler: Example,
        repo: @repo,
        name: "job",
        max_retries: 5,
        autoremove: true,
        state: %{key: :value}
      )

      inserted_job = @repo.one!(Job)

      assert inserted_job.autoremove
      assert inserted_job.max_retries == 5
      assert inserted_job.schedule == "*/1 * * * *"
      assert inserted_job.state == %{key: :value}
    end

    test "does not raise if job with name exists" do
      params = [
        handler: Example,
        name: "job",
        repo: @repo,
        schedule: DateTime.utc_now()
      ]

      Cue.create_job_unless_exists!(params)
      Cue.create_job_unless_exists!(params)
    end
  end
end
