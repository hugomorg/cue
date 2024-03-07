defmodule CueTest do
  use Cue.DataCase
  doctest Cue
  @repo Cue.TestRepo
  alias Cue.Job

  setup do
    # This is to send messages from the callbacks, thus
    # confirming that they have been invoked
    Agent.start(fn -> nil end, name: __MODULE__)
    :ok
  end

  @name_to_trigger_error "JPY-VND"
  @name_to_trigger_crash "CAD-SGD"
  @name_to_trigger_success "GBP-EUR"

  defmodule Example do
    use Cue, schedule: "* * * * * *"

    def init(_name) do
      {:ok, %{id: 0}}
    end

    def handle_job("CAD-SGD", _state) do
      raise "uh oh"
    end

    def handle_job("JPY-VND", _state) do
      {:error, "yo"}
    end

    def handle_job("GBP-EUR", state) do
      Agent.update(CueTest, fn _ -> "GBP-EUR" end)
      {:ok, %{state | id: state.id + 1}}
    end

    def handle_job(_name, _state) do
      :ok
    end

    def handle_job_error(name, _state, _error_info) do
      Agent.update(CueTest, fn _ -> name end)
      :ok
    end
  end

  defmodule Example2 do
    use Cue

    def handle_job("fail", _state) do
      {:error, :fail}
    end

    def handle_job(_name, _state) do
      :ok
    end

    def handle_job_error(_name, _state, _error_info) do
      :ok
    end
  end

  describe "create_job!/1" do
    test "successfully inserts job into table - assumed module defaults" do
      assert %{name: "CueTest.Example", run_at: %DateTime{}} = Example.create_job!()
      assert job = @repo.one!(Job)
      assert job.name == "CueTest.Example"
      assert job.state == %{id: 0}
      assert job.handler == CueTest.Example
      refute job.last_error
      assert job.schedule == "* * * * * *"
      assert DateTime.compare(job.run_at, DateTime.utc_now()) == :gt
      refute job.last_succeeded_at
      refute job.last_failed_at
      assert job.status == :not_started
    end

    test "options can be overridden" do
      assert Example.create_job!(name: "GBP-EUR", schedule: "*/45 * * * *")
      assert job = @repo.one!(Job)
      assert job.name == "GBP-EUR"
      assert job.schedule == "*/45 * * * *"
    end

    @tag :slow
    test "successful job is updated properly" do
      now = DateTime.utc_now()
      Example.create_job!(name: @name_to_trigger_success)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert job.state.id > 0
      assert DateTime.compare(job.last_succeeded_at, now) == :gt
      assert job.status == :succeeded
      refute job.last_error
      refute job.last_failed_at
      assert Agent.get(CueTest, &Function.identity/1) == @name_to_trigger_success
    end

    @tag :slow
    test "schedule can be datetime" do
      now = DateTime.utc_now()
      Example.create_job!(name: @name_to_trigger_success, schedule: now)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert job.state.id > 0
      assert DateTime.compare(job.last_succeeded_at, now) == :gt
      assert job.status == :succeeded
    end

    @tag :slow
    test "job returning error is updated properly" do
      now = DateTime.utc_now()
      Example.create_job!(name: @name_to_trigger_error)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert DateTime.compare(job.last_failed_at, now) == :gt
      assert job.last_error == "yo"
      assert job.status == :failed
      refute job.last_succeeded_at
      assert Agent.get(CueTest, &Function.identity/1) == @name_to_trigger_error
    end

    @tag :slow
    test "max_retries works" do
      now = DateTime.utc_now()
      Example.create_job!(name: @name_to_trigger_error, max_retries: 1)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert DateTime.compare(job.last_failed_at, now) == :gt
      assert job.last_error == "yo"
      assert job.status == :paused
      assert job.retry_count == 1
    end

    @tag :slow
    test "job which crashed is updated properly" do
      now = DateTime.utc_now()
      Example.create_job!(name: @name_to_trigger_crash)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert DateTime.compare(job.last_failed_at, now) == :gt
      assert job.last_error =~ "uh oh"
      assert job.status == :failed
      refute job.last_succeeded_at
      assert Agent.get(CueTest, &Function.identity/1) == @name_to_trigger_crash
    end

    test "schedule doesn't need to be defined at module" do
      assert Example2.create_job!(schedule: DateTime.utc_now())
    end

    test "one-off jobs are not retried" do
      assert Example2.create_job!(schedule: DateTime.utc_now(), name: "fail")
      @repo.one!(Job)
      :timer.sleep(1000)
      assert @repo.one!(Job).retry_count == 0
    end

    test "one-off jobs are removed after running if autoremove is true" do
      assert Example2.create_job!(schedule: DateTime.utc_now(), autoremove: true)
      @repo.one!(Job)
      :timer.sleep(1000)
      refute @repo.exists?(Job)
    end

    test "scheduled jobs are not removed even if autoremove is true" do
      assert Example2.create_job!(schedule: "* * * * * *", autoremove: true)
      @repo.one!(Job)
      :timer.sleep(1000)
      assert @repo.exists?(Job)
    end
  end

  describe "create_job/1" do
    test "successfully inserts job into table - assumed module defaults" do
      assert {:ok, %{name: "CueTest.Example", run_at: %DateTime{}}} = Example.create_job()
      assert job = @repo.one!(Job)
      assert job.name == "CueTest.Example"
      assert job.state == %{id: 0}
      assert job.handler == CueTest.Example
      refute job.last_error
      assert job.schedule == "* * * * * *"
      assert DateTime.compare(job.run_at, DateTime.utc_now()) == :gt
      refute job.last_succeeded_at
      refute job.last_failed_at
      assert job.status == :not_started
    end

    test "returns error if job exists" do
      Example.create_job()
      assert Example.create_job() == {:error, {:job_exists, "CueTest.Example"}}
    end

    test "returns error if cron invalid" do
      assert {:error, {:invalid_schedule, _msg}} = Example.create_job(schedule: "* * *")
    end

    test "returns error if schedule invalid" do
      assert {:error, {:invalid_schedule, _msg}} = Example.create_job(schedule: 2)
    end

    test "returns error if handler invalid" do
      assert {:error, {:invalid_handler, _msg}} = Example.create_job(handler: :yo)
    end
  end

  describe "remove_job/2" do
    test "by default removes assumed job" do
      Example.create_job!()
      assert @repo.exists?(Job)
      Example.remove_job()
      refute @repo.exists?(Job)
    end

    test "by default removes with given name" do
      Example.create_job!(name: @name_to_trigger_success)
      assert @repo.exists?(Job)
      Example.remove_job(@name_to_trigger_success)
      refute @repo.exists?(Job)
    end
  end
end
