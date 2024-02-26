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

  describe "enqueue!/1" do
    test "successfully inserts job into table - assumed module defaults" do
      assert Example.enqueue!() == "CueTest.Example"
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
      assert Example.enqueue!(name: "GBP-EUR", schedule: "*/45 * * * *")
      assert job = @repo.one!(Job)
      assert job.name == "GBP-EUR"
      assert job.schedule == "*/45 * * * *"
    end

    @tag :slow
    test "successful job is updated properly" do
      now = DateTime.utc_now()
      Example.enqueue!(name: @name_to_trigger_success)
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
      Example.enqueue!(name: @name_to_trigger_success, schedule: now)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert job.state.id > 0
      assert DateTime.compare(job.last_succeeded_at, now) == :gt
      assert job.status == :succeeded
    end

    @tag :slow
    test "job returning error is updated properly" do
      now = DateTime.utc_now()
      Example.enqueue!(name: @name_to_trigger_error)
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
      Example.enqueue!(name: @name_to_trigger_error, max_retries: 1)
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
      Example.enqueue!(name: @name_to_trigger_crash)
      :timer.sleep(2000)
      assert job = @repo.one!(Job)
      assert DateTime.compare(job.last_failed_at, now) == :gt
      assert job.last_error =~ "uh oh"
      assert job.status == :failed
      refute job.last_succeeded_at
      assert Agent.get(CueTest, &Function.identity/1) == @name_to_trigger_crash
    end
  end
end
