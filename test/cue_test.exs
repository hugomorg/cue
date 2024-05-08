defmodule CueTest do
  alias Cue.Scheduler
  use Cue.DataCase
  doctest Cue
  @repo Cue.TestRepo
  alias Cue.Job

  import Hammox

  setup :verify_on_exit!

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

  defmodule ExampleMinimal do
    use Cue, schedule: "*/2 * * * *"
    def handle_job(_, _), do: :ok

    def handle_job_error(_, _, _), do: :ok
  end

  defmodule ExampleWithOpts do
    use Cue,
      name: "Hard worker",
      schedule: "*/2 * * * *",
      autoremove: true,
      max_retries: 100

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

  describe "__using__/1" do
    test "validates missing opts" do
      assert_raise Cue.Error, fn ->
        Example.create_job!()
      end
    end

    test "create_job!/1 is defined on bare minimum example" do
      job = ExampleMinimal.create_job!()

      inserted_job = @repo.one!(Job)

      assert job.name == "CueTest.ExampleMinimal"
      assert job.name == inserted_job.name
      assert inserted_job.retry_count == 0
      assert inserted_job.run_at == job.run_at
      assert inserted_job.status == :not_started
      assert inserted_job.schedule == "*/2 * * * *"

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.state
    end

    test "create_job/1 is defined on bare minimum example" do
      {:ok, job} = ExampleMinimal.create_job()

      inserted_job = @repo.one!(Job)

      assert job.name == "CueTest.ExampleMinimal"
      assert job.name == inserted_job.name
      assert inserted_job.schedule == "*/2 * * * *"

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.state
    end

    test "create_job_unless_exists!/1 is defined on bare minimum example" do
      job = ExampleMinimal.create_job_unless_exists!()

      inserted_job = @repo.one!(Job)

      assert job.name == "CueTest.ExampleMinimal"
      assert job.name == inserted_job.name
      assert inserted_job.schedule == "*/2 * * * *"

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.state
    end

    test "create_job_unless_exists/1 is defined on bare minimum example" do
      {:ok, job} = ExampleMinimal.create_job_unless_exists()

      inserted_job = @repo.one!(Job)

      assert job.name == "CueTest.ExampleMinimal"
      assert job.name == inserted_job.name
      assert inserted_job.schedule == "*/2 * * * *"

      refute inserted_job.autoremove
      refute inserted_job.last_error
      refute inserted_job.last_failed_at
      refute inserted_job.last_succeeded_at
      refute inserted_job.max_retries
      refute inserted_job.state
    end

    test "options are merged in" do
      job =
        ExampleMinimal.create_job!(
          schedule: "*/1 * * * *",
          handler: Example,
          repo: @repo,
          name: "job",
          max_retries: 5,
          autoremove: true,
          state: %{key: :value}
        )

      inserted_job = @repo.one!(Job)

      assert job.name == "job"
      assert job.name == inserted_job.name
      assert inserted_job.schedule == "*/1 * * * *"
      assert inserted_job.autoremove
      assert inserted_job.max_retries == 5
      assert inserted_job.state == %{key: :value}
    end

    test "remove is defined and by default is scoped to jobs defined by module" do
      expect(Cue.Scheduler.Mock, :add_jobs_to_ignored, fn ["job1", "job2"] -> :ok end)
      expect(Cue.Scheduler.Mock, :remove_jobs_from_ignored, fn ["job1", "job2"] -> :ok end)

      job_1 = ExampleMinimal.create_job!(name: "job1")
      job_2 = ExampleMinimal.create_job!(name: "job2")
      job_3 = ExampleWithOpts.create_job!(name: "job3")

      ExampleMinimal.remove_jobs()

      refute @repo.get_by(Job, name: job_1.name)
      refute @repo.get_by(Job, name: job_2.name)
      assert @repo.get_by(Job, name: job_3.name)
    end
  end

  describe "remove_all_jobs/1" do
    test "deletes all jobs" do
      expect(Cue.Scheduler.Mock, :pause, fn -> :ok end)
      expect(Cue.Scheduler.Mock, :resume, fn -> :ok end)

      params = [schedule: DateTime.utc_now(), handler: Example, repo: @repo, name: "job1"]
      Cue.create_job!(params)
      Cue.create_job!(Keyword.put(params, :name, "job2"))
      Cue.create_job!(Keyword.put(params, :name, "job3"))
      Cue.create_job!(Keyword.put(params, :name, "job4"))

      assert Cue.remove_all_jobs(@repo) == 4

      refute @repo.exists?(Job)
    end
  end

  describe "remove_job/2" do
    test "removes job by name and returns delete count" do
      expect(Cue.Scheduler.Mock, :add_job_to_ignored, fn "job" -> :ok end)
      expect(Cue.Scheduler.Mock, :remove_job_from_ignored, fn "job" -> :ok end)

      assert job =
               Cue.create_job!(
                 schedule: DateTime.utc_now(),
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      assert Cue.remove_job(@repo, job.name) == 1

      refute @repo.exists?(Job)
    end
  end

  describe "remove_job_by/3" do
    test "removes job by name if given string" do
      expect(Cue.Scheduler.Mock, :add_job_to_ignored, fn "job" -> :ok end)
      expect(Cue.Scheduler.Mock, :remove_job_from_ignored, fn "job" -> :ok end)

      assert job =
               Cue.create_job!(
                 schedule: DateTime.utc_now(),
                 handler: Example,
                 repo: @repo,
                 name: "job"
               )

      assert Cue.remove_jobs_by(@repo, :name, job.name) == 1

      refute @repo.exists?(Job)
    end

    test "supports like patterns for name" do
      expect(Cue.Scheduler.Mock, :add_jobs_to_ignored, fn ["job1", "job2"] -> :ok end)
      expect(Cue.Scheduler.Mock, :remove_jobs_from_ignored, fn ["job1", "job2"] -> :ok end)
      params = [schedule: DateTime.utc_now(), handler: Example, repo: @repo, name: "job1"]
      job_1 = Cue.create_job!(params)
      job_2 = Cue.create_job!(Keyword.put(params, :name, "job2"))
      job_3 = Cue.create_job!(Keyword.put(params, :name, "JOB3"))
      job_4 = Cue.create_job!(Keyword.put(params, :name, "4job"))

      assert Cue.remove_jobs_by(@repo, :name, like: "job%") == 2

      refute @repo.get_by(Job, name: job_1.name)
      refute @repo.get_by(Job, name: job_2.name)
      assert @repo.get_by(Job, name: job_3.name)
      assert @repo.get_by(Job, name: job_4.name)
    end

    test "supports ilike patterns for name" do
      expect(Cue.Scheduler.Mock, :add_jobs_to_ignored, fn ["job1", "job2", "JOB3"] -> :ok end)

      expect(Cue.Scheduler.Mock, :remove_jobs_from_ignored, fn ["job1", "job2", "JOB3"] -> :ok end)

      params = [schedule: DateTime.utc_now(), handler: Example, repo: @repo, name: "job1"]
      job_1 = Cue.create_job!(params)
      job_2 = Cue.create_job!(Keyword.put(params, :name, "job2"))
      job_3 = Cue.create_job!(Keyword.put(params, :name, "JOB3"))
      job_4 = Cue.create_job!(Keyword.put(params, :name, "4job"))

      assert Cue.remove_jobs_by(@repo, :name, ilike: "job%") == 3

      refute @repo.get_by(Job, name: job_1.name)
      refute @repo.get_by(Job, name: job_2.name)
      refute @repo.get_by(Job, name: job_3.name)
      assert @repo.get_by(Job, name: job_4.name)
    end

    test "deletes by handler" do
      expect(Cue.Scheduler.Mock, :add_jobs_to_ignored, fn ["job1", "job2"] -> :ok end)

      expect(Cue.Scheduler.Mock, :remove_jobs_from_ignored, fn ["job1", "job2"] -> :ok end)

      params = [schedule: DateTime.utc_now(), handler: Example, repo: @repo, name: "job1"]
      job_1 = Cue.create_job!(params)
      job_2 = Cue.create_job!(Keyword.put(params, :name, "job2"))
      job_3 = Cue.create_job!(Keyword.merge(params, name: "job3", handler: ExampleWithOpts))
      job_4 = Cue.create_job!(Keyword.merge(params, name: "job4", handler: ExampleWithOpts))

      assert Cue.remove_jobs_by(@repo, :handler, Example) == 2

      refute @repo.get_by(Job, name: job_1.name)
      refute @repo.get_by(Job, name: job_2.name)
      assert @repo.get_by(Job, name: job_3.name)
      assert @repo.get_by(Job, name: job_4.name)
    end
  end
end
