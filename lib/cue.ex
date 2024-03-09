defmodule Cue do
  @moduledoc """
  Cue helps you "queue" up, schedule and run tasks.

  Please see README.md for more information including setup, this is just an intro.

  You define a job like so:

  ```
  defmodule YourApp do
    use Cue, schedule: "* * * * *"

    @impl true
    def handle_job(name, state) do
      # Business logic goes here...
      :ok
    end

    @impl true
    def handle_job_error(name, state, error_info) do
      # Do something with the error
      :ok
    end
  end
  ```

  No options are required until you create the job.

  It is possible to override any/all of the "module options" whenever `create_job/1` or `create_job!/1` are called.

  There are two main types of functions which are defined on the module, to handle creating and removing jobs.

  `YourApp.create_job/1` / `YourApp.create_job!/1` actually create the job at the database level. The job will not start running until this happens.

  To remove a job, just call `YourApp.remove_job()` which defaults to the module name but if passed a name removes the corresponding job.

  If you need further customisation, you can call `Cue.create_job/1` / `Cue.create_job!/1` / `Cue.remove_job/2`.
  """

  @type name :: String.t()
  @type state :: any()
  @callback init(name) :: {:ok, any()}
  @callback handle_job(name, state) :: :ok | {:ok, map()} | {:error, any()}
  @type error_info :: %{
          error: String.t(),
          retry_count: non_neg_integer(),
          max_retries: non_neg_integer()
        }
  @callback handle_job_error(name, state, error_info) :: :ok | {:ok, map()} | {:error, any()}

  @optional_callbacks [init: 1]

  @type run_at :: DateTime.t()
  @type create_job_return :: %{name: name, run_at: run_at}

  @opts [
    :handler,
    :repo,
    :name,
    :schedule,
    :autoremove,
    max_retries: nil,
    state: nil
  ]

  alias Cue.Job

  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [{Task.Supervisor, name: Cue.TaskProcessor}, Cue.Scheduler]

    children
    |> maybe_start_test_repo()
    |> Supervisor.init(strategy: :one_for_one)
  end

  defp maybe_start_test_repo(children) do
    if Application.get_env(:cue, :start_test_repo?, false) do
      [Cue.TestRepo] ++ children
    else
      children
    end
  end

  @doc """
  Inserts a job into the database.

  Options:

  - `schedule`: a string representing a cron specification, or a UTC `DateTime` value.
  If it is the latter, it means the job will only be processed once, at that given time.
  If a cron spec, the job will be repeated.

  - `name`: must be a string, and unique across all jobs. Defaults to the module name.

  - `autoremove`: should be a boolean. Controls whether one-off jobs are deleted after running (whether successful or not).
  Defaults to `false`.

  - `max_retries`: how many times the job should be retried in the event of a failure.
  The retry count gets reset after a successful run.
  Defaults to `nil`, which means the job will keep retrying.

  - `handler`: a loaded module which defines `handle_job/2` and `handle_job_error/3` callbacks

  Returns the name of the job and the time the job will run next.
  """
  @spec create_job!(keyword()) :: create_job_return
  def create_job!(opts) do
    opts = Keyword.validate!(opts, @opts)

    handler = validate_job_handler!(opts[:handler])

    state = init_job(opts[:name], handler)

    {schedule, run_at} = validate_schedule!(opts[:schedule])

    job =
      %Job{}
      |> Job.changeset(%{
        name: opts[:name],
        handler: handler,
        run_at: run_at,
        schedule: schedule,
        status: :not_started,
        max_retries: opts[:max_retries],
        state: state,
        autoremove: opts[:autoremove]
      })
      |> opts[:repo].insert!()

    %{name: job.name, run_at: job.run_at}
  end

  @doc """
  Same as `create_job!/1` but does not raise if there is an error.
  """
  @spec create_job(keyword()) :: {:error, {atom(), String.t()}} | {:ok, create_job_return}
  def create_job(opts) do
    with {:ok, opts} <- Keyword.validate(opts, @opts),
         {:ok, {schedule, run_at}} <- validate_schedule(opts[:schedule]),
         {:ok, handler} <- validate_job_handler(opts[:handler]) do
      state = init_job(opts[:name], handler)

      case %Job{}
           |> Job.changeset(%{
             name: opts[:name],
             handler: handler,
             run_at: run_at,
             schedule: schedule,
             status: :not_started,
             max_retries: opts[:max_retries],
             autoremove: opts[:autoremove],
             state: state
           })
           |> opts[:repo].insert() do
        {:ok, job} ->
          {:ok, %{name: job.name, run_at: job.run_at}}

        {:error,
         %{errors: [name: {_msg, [constraint: :unique, constraint_name: "cue_jobs_name_index"]}]}} ->
          {:error, {:job_exists, opts[:name]}}
      end
    end
  end

  @doc """
  Deletes the job from the database.

  Accepts the repo and job name as arguments.
  """
  @spec remove_job(atom(), name()) :: non_neg_integer()
  def remove_job(repo, job_name) do
    require Ecto.Query

    # Synchronously remove job from scheduling/processing
    Cue.Scheduler.add_job_to_ignored(job_name)

    {count, _returned} =
      Job
      |> Ecto.Query.where(name: ^job_name)
      |> repo.delete_all()

    Cue.Scheduler.remove_job_from_ignored(job_name)

    count
  end

  def list_jobs(repo) do
    require Ecto.Query

    Job
    |> Ecto.Query.order_by(desc: :run_at)
    |> Ecto.Query.select([j], %{
      run_at: j.run_at,
      retry_count: j.retry_count,
      last_failed_at: j.last_failed_at,
      last_succeeded_at: j.last_succeeded_at,
      last_error: j.last_error,
      max_retries: j.max_retries,
      autoremove: j.autoremove,
      state: j.state,
      status: j.status,
      schedule: j.schedule,
      handler: j.handler,
      name: j.name
    })
    |> repo.all()
  end

  defp validate_job_handler!(handler) when is_atom(handler) do
    module_exists? = Code.ensure_loaded?(handler)
    job_handler_defined? = function_exported?(handler, :handle_job, 2)
    error_handler_defined? = function_exported?(handler, :handle_job_error, 3)

    cond do
      module_exists? and job_handler_defined? and error_handler_defined? ->
        handler

      module_exists? and job_handler_defined? ->
        raise handle_job_error_error_message(handler)

      :else ->
        raise handle_job_error_message(handler)
    end
  end

  defp validate_job_handler(handler) when is_atom(handler) do
    module_exists? = Code.ensure_loaded?(handler)
    job_handler_defined? = function_exported?(handler, :handle_job, 2)
    error_handler_defined? = function_exported?(handler, :handle_job_error, 3)

    cond do
      module_exists? and job_handler_defined? and error_handler_defined? ->
        {:ok, handler}

      module_exists? and job_handler_defined? ->
        {:error, {:invalid_handler, handle_job_error_error_message(handler)}}

      :else ->
        {:error, {:invalid_handler, handle_job_error_message(handler)}}
    end
  end

  defp validate_job_handler(_handler) do
    {:error, {:invalid_handler, "Handler must be an atom"}}
  end

  defp init_job(name, module) do
    if function_exported?(module, :init, 1) do
      case module.init(name) do
        {:ok, state} ->
          state

        unexpected_return ->
          raise "You must return `{:ok, state}` from your `init/1` function, received #{inspect(unexpected_return)}"
      end
    end
  end

  defp validate_schedule!(schedule) when is_binary(schedule) do
    {schedule, Job.next_run_at!(schedule)}
  end

  defp validate_schedule!(%DateTime{} = schedule) do
    {nil, schedule}
  end

  defp validate_schedule!(schedule) do
    raise schedule_error_message(schedule)
  end

  defp validate_schedule(schedule) when is_binary(schedule) do
    case Job.next_run_at(schedule) do
      {:ok, next_run_at} ->
        {:ok, {schedule, next_run_at}}

      :error ->
        {:error, {:invalid_schedule, schedule_error_message(schedule)}}

      error ->
        error
    end
  end

  defp validate_schedule(%DateTime{} = schedule) do
    {:ok, {nil, schedule}}
  end

  defp validate_schedule(schedule) do
    {:error, {:invalid_schedule, schedule_error_message(schedule)}}
  end

  defp schedule_error_message(schedule) do
    "Schedule must be a valid cron specification or a UTC datetime, received #{inspect(schedule)}"
  end

  defp handle_job_error_error_message(handler) do
    "Please define `handle_job_error/3` in #{inspect(handler)}"
  end

  defp handle_job_error_message(handler) do
    "Please define `handle_job/2` in #{inspect(handler)}"
  end

  defmacro __using__(opts) do
    name = Keyword.get(opts, :name)
    schedule = Keyword.get(opts, :schedule)
    autoremove = Keyword.get(opts, :autoremove, false)
    max_retries = Keyword.get(opts, :max_retries)

    quote do
      @behaviour Cue
      @repo Application.compile_env(:cue, :repo)
      @cue_name unquote(name) || String.replace("#{__MODULE__}", ~r/^Elixir\./, "")

      @doc """
      Inserts a job in the database.

      The options passed are merged into the module options.

      For example if you had defined:

      ```
      defmodule YourApp do
        use Cue, schedule: "* * * * *"
        ...
      end
      ```

      Then you could override the schedule on a per-job basis with `YourApp.create_job!(name: "some name", schedule: DateTime.utc_now())`.

      Options:

      - `schedule`: a string representing a cron specification, or a UTC `DateTime` value.
      If it is the latter, it means the job will only be processed once, at that given time.
      If a cron spec, the job will be repeated.

      - `name`: must be a string, and unique across all jobs. Defaults to the module name.

      - `autoremove`: should be a boolean. Controls whether one-off jobs are deleted after running (whether successful or not).
      Defaults to `false`.

      - `max_retries`: how many times the job should be retried in the event of a failure.
      The retry count gets reset after a successful run.
      Defaults to `nil`, which means the job will keep retrying.

      The repo used is the one defined in the config.
      """
      def create_job!(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo,
          autoremove: unquote(autoremove),
          max_retries: unquote(max_retries)
        ]
        |> Keyword.merge(opts)
        |> Cue.create_job!()
      end

      @doc """
      Same as `create_job!/1`, but does not raise, returning `{:error, reason}` if there is a problem.
      """
      def create_job(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo,
          autoremove: unquote(autoremove),
          max_retries: unquote(max_retries)
        ]
        |> Keyword.merge(opts)
        |> Cue.create_job()
      end

      @doc """
      Deletes the job from the database.

      Assumes name is the module name, unless a name is provided.

      Uses the repo defined in config.
      """
      def remove_job(name \\ @cue_name) do
        Cue.remove_job(@repo, name)
      end
    end
  end
end
