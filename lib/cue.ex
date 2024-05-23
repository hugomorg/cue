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

  No options (e.g. `schedule`) are required until you create the job.

  It is possible to override any/all of the "module options" whenever `create_job/1` or `create_job!/1` are called.

  There are two main types of functions which are defined on the module, to handle creating and removing jobs.

  `YourApp.create_job/1` / `YourApp.create_job!/1` / `YourApp.create_job_unless_exists/1` / `YourApp.create_job_unless_exists!` actually create the job at the database level.

  The job will not start running until this happens.

  To remove all jobs created by the module, just call `YourApp.remove_jobs/1`.

  If you need more fine-grained control with removal, check out `Cue.remove_jobs/1`.
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
    max_retries: nil,
    state: nil
  ]

  @no_conflict_opts [conflict_target: :name, on_conflict: :nothing]

  alias Cue.Job
  import Ecto.Query

  use Supervisor

  @repo Application.compile_env!(:cue, :repo)

  def start_link do
    Supervisor.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [{Task.Supervisor, name: Cue.TaskProcessor}, Cue.Scheduler.impl()]

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

  - `state`: any Elixir term passed to the handler. This is useful if you want to track something across runs.

  - `max_retries`: how many times the job should be retried in the event of a failure.
  The retry count gets reset after a successful run.
  Defaults to `nil`, which means the job will retry infinitely, on its given schedule.
  If the job is a "one-off" job, it will not get retried.

  - `handler`: a module which should define `handle_job/2` and `handle_job_error/3` callbacks.

  This function returns the name of the job and the time the job will run next.
  """
  @spec create_job!(keyword()) :: create_job_return
  def create_job!(opts) do
    {changeset, validated_opts} = prepare_job!(opts)
    job = validated_opts[:repo].insert!(changeset)

    %{name: job.name, run_at: job.run_at}
  end

  @doc """
  Takes exactly the same options as `create_job!/1`.
  The only difference here is that if the job already exists, there is no error raised.

  This is really useful if you don't care whether the job exists or not.

  This won't override any options. If you want to actually change a job's options
  you should remove the job and re-create.
  """
  @spec create_job_unless_exists!(keyword()) :: create_job_return
  def create_job_unless_exists!(opts) do
    {changeset, validated_opts} = prepare_job!(opts)
    job = validated_opts[:repo].insert!(changeset, @no_conflict_opts)

    %{name: job.name, run_at: job.run_at}
  end

  @doc """
  Same as `create_job!/1` but does not raise if there is an error.
  """
  @spec create_job(keyword()) :: {:error, {atom(), String.t()}} | {:ok, create_job_return}
  def create_job(opts) do
    with {:ok, changeset = %Ecto.Changeset{}, validated_opts} <- prepare_job(opts) do
      case validated_opts[:repo].insert(changeset) do
        {:ok, job} ->
          {:ok, %{name: job.name, run_at: job.run_at}}

        {:error,
         %{errors: [name: {_msg, [constraint: :unique, constraint_name: "cue_jobs_name_index"]}]}} ->
          {:error, {:job_exists, validated_opts[:name]}}
      end
    end
  end

  @doc """
  Same as `create_job_unless_exists!/1` but does not raise if there is an error.
  """
  @spec create_job_unless_exists(keyword()) ::
          {:error, {atom(), String.t()}} | {:ok, create_job_return}
  def create_job_unless_exists(opts) do
    with {:ok, changeset = %Ecto.Changeset{}, validated_opts} <- prepare_job(opts) do
      case validated_opts[:repo].insert(changeset, @no_conflict_opts) do
        {:ok, job} ->
          {:ok, %{name: job.name, run_at: job.run_at}}

        {:error, error} ->
          {:error, error}
      end
    end
  end

  @doc """
  Removes every single job in the database.
  """
  @spec remove_all_jobs() :: non_neg_integer()
  @spec remove_all_jobs(atom()) :: non_neg_integer()
  def remove_all_jobs(repo \\ @repo) do
    Cue.Scheduler.impl().pause()
    {count, _returned} = repo.delete_all(Job)
    Cue.Scheduler.impl().resume()

    count
  end

  @doc """
  Deletes the job from the database.

  Accepts the repo and job name as arguments.

  Use this function is you want to remove a single job only.
  """
  @spec remove_job(name()) :: non_neg_integer()
  @spec remove_job(atom(), name()) :: non_neg_integer()
  def remove_job(repo \\ @repo, job_name) do
    # Synchronously remove job from scheduling/processing
    Cue.Scheduler.impl().add_job_to_ignored(job_name)

    {count, _returned} =
      Job
      |> Ecto.Query.where(name: ^job_name)
      |> repo.delete_all()

    Cue.Scheduler.impl().remove_job_from_ignored(job_name)

    count
  end

  @doc """
  This function can be used to remove multiple jobs by some condition.

  For example, let's say you are namespacing your jobs, and a name pattern you have is: `"currency.europe.*"`. The `"*"` could be GBP, EUR etc.
  And you might also have `"currency.americas.*"`.

  If you wanted to clear out the European currency jobs you can simply call `Cue.remove_jobs(where: [name: [ilike: "currency.europe%"]])`.

  You can use the same matching patterns as `"LIKE"`/`"ILIKE"` database functions. If you want to be stricter with case, swap out `:ilike` for `:like`.

  Another thing you want to do is remove jobs by the given handler. For that you can use `Cue.remove_jobs(where: [handler: SomeHandler])`. Or more simply, `SomeHandler.remove_jobs()`.

  For more information about filtering, see `list_jobs/2` below.
  """
  @spec remove_jobs(keyword()) :: non_neg_integer()
  def remove_jobs(opts) when is_list(opts) do
    repo = Keyword.get(opts, :repo, @repo)
    where = Keyword.fetch!(opts, :where)

    jobs =
      Job
      |> maybe_filter_query(where)
      |> repo.all()

    job_names = Enum.map(jobs, & &1.name)

    Cue.Scheduler.impl().add_jobs_to_ignored(job_names)

    callback = opts[:callback]

    if is_function(callback, 1) do
      Enum.each(jobs, callback)
    end

    {count, _returned} =
      Job
      |> where([j], j.id in ^Enum.map(jobs, & &1.id))
      |> repo.delete_all()

    Cue.Scheduler.impl().remove_jobs_from_ignored(job_names)

    count
  end

  @doc """
  Returns all jobs. You can add some filtering conditions to narrow down the search and also change the order.

  Let's use some examples. If you wanted to find only failed jobs earlier than some point, that had the name matching the wildcard `"fx.*"`, sorted by `last_failed_at` and then name, you could do:

  ```
  list_jobs(
    where: [status: :failed, name: [ilike: "fx.%"], failed_at: [<: timestamp]],
    order_by: [desc: :failed_at, asc: :name]
  )
  ```

  Bear in mind the query options are just a simple wrapper over Ecto and has its limitations.

  You might need to drop down to Ecto queries direct to do something more custom.
  """
  @spec list_jobs(list()) :: [
          %{
            handler: atom(),
            last_error: nil | String.t(),
            last_failed_at: DateTime.t(),
            last_succeeded_at: DateTime.t(),
            max_retries: non_neg_integer(),
            name: String.t(),
            retry_count: non_neg_integer(),
            run_at: DateTime.t(),
            schedule: nil | String.t(),
            state: any(),
            status: :not_started | :processing | :failed | :succeeded | :paused
          }
        ]
  def list_jobs(opts \\ []) do
    order_by = Keyword.get(opts, :order_by, desc: :run_at)
    where = Keyword.get(opts, :where)
    repo = Keyword.get(opts, :repo, @repo)

    Job
    |> order_by(^order_by)
    |> maybe_filter_query(where)
    |> select([j], %{
      handler: j.handler,
      last_error: j.last_error,
      last_failed_at: j.last_failed_at,
      last_succeeded_at: j.last_succeeded_at,
      max_retries: j.max_retries,
      name: j.name,
      retry_count: j.retry_count,
      run_at: j.run_at,
      schedule: j.schedule,
      state: j.state,
      status: j.status
    })
    |> repo.all()
  end

  defp validate_repo(repo) do
    if Code.ensure_loaded?(repo) do
      :ok
    else
      {:error, :no_repo}
    end
  end

  defp maybe_filter_query(query, nil) do
    query
  end

  # Recursively build where expressions
  defp maybe_filter_query(query, where) when is_list(where) do
    Enum.reduce(where, query, fn cond, q ->
      maybe_filter_query(q, cond)
    end)
  end

  defp maybe_filter_query(query, {key, [ilike: pattern]}) do
    where(query, [j], ilike(field(j, ^key), ^pattern))
  end

  defp maybe_filter_query(query, {key, [like: pattern]}) do
    where(query, [j], like(field(j, ^key), ^pattern))
  end

  defp maybe_filter_query(query, {key, [>: gt]}) do
    where(query, [j], field(j, ^key) > ^gt)
  end

  defp maybe_filter_query(query, {key, [>=: gte]}) do
    where(query, [j], field(j, ^key) >= ^gte)
  end

  defp maybe_filter_query(query, {key, [<: lt]}) do
    where(query, [j], field(j, ^key) < ^lt)
  end

  defp maybe_filter_query(query, {key, [<=: lte]}) do
    where(query, [j], field(j, ^key) <= ^lte)
  end

  defp maybe_filter_query(query, {key, value}) when is_list(value) do
    where(query, [j], field(j, ^key) in ^value)
  end

  defp maybe_filter_query(query, {key, value}) do
    where(query, [j], field(j, ^key) == ^value)
  end

  defp prepare_job(opts) do
    with {:ok, validated_opts} <- Keyword.validate(opts, @opts),
         {:ok, {schedule, run_at}} <- validate_schedule(validated_opts[:schedule]),
         {:ok, handler} <- validate_job_handler(validated_opts[:handler]),
         :ok <- validate_repo(validated_opts[:repo]),
         :ok <- validate_name(validated_opts[:name]) do
      state = Keyword.get_lazy(opts, :state, fn -> init_job(validated_opts[:name], handler) end)

      {:ok,
       %Job{}
       |> Job.changeset(%{
         name: validated_opts[:name],
         handler: handler,
         run_at: run_at,
         schedule: schedule,
         status: :not_started,
         max_retries: validated_opts[:max_retries],
         state: state
       }), validated_opts}
    end
  end

  defp prepare_job!(opts) do
    validated_opts = Keyword.validate!(opts, @opts)

    handler = validate_job_handler!(validated_opts[:handler])

    unless Code.ensure_loaded?(validated_opts[:repo]),
      do: raise(__MODULE__.Error, message: "No repo")

    unless validated_opts[:name], do: raise(__MODULE__.Error, message: "No name")
    unless is_binary(validated_opts[:name]), do: raise(__MODULE__.Error, message: "Invalid name")

    state = Keyword.get_lazy(opts, :state, fn -> init_job(opts[:name], handler) end)

    {schedule, run_at} = validate_schedule!(validated_opts[:schedule])

    job =
      Job.changeset(%Job{}, %{
        name: validated_opts[:name],
        handler: handler,
        run_at: run_at,
        schedule: schedule,
        status: :not_started,
        max_retries: validated_opts[:max_retries],
        state: state
      })

    {job, validated_opts}
  end

  defp validate_name(nil), do: {:error, :no_name}

  defp validate_name(name) do
    if is_binary(name) do
      :ok
    else
      {:error, {:invalid_name, "Must be string"}}
    end
  end

  defp validate_job_handler!(handler) when is_atom(handler) do
    case validate_job_handler(handler) do
      {:ok, handler} -> handler
      {:error, {:invalid_handler, msg}} -> raise __MODULE__.Error, message: msg
    end
  end

  defp validate_job_handler(nil), do: {:error, {:invalid_handler, "Please define a handler"}}

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
          raise Cue.Error,
                "You must return `{:ok, state}` from your `init/1` function, received #{inspect(unexpected_return)}"
      end
    end
  end

  defp validate_schedule!(schedule) when is_binary(schedule) do
    {schedule, Job.next_run_at!(schedule)}
  end

  defp validate_schedule!(schedule = %DateTime{}) do
    {nil, schedule}
  end

  defp validate_schedule!(schedule) do
    raise Cue.Error, schedule_error_message(schedule)
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

  defp validate_schedule(schedule = %DateTime{}) do
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
    max_retries = Keyword.get(opts, :max_retries)

    quote do
      @behaviour Cue
      @repo Application.compile_env!(:cue, :repo)
      @cue_name unquote(name) || String.replace("#{__MODULE__}", ~r/^Elixir\./, "")

      @doc """
      Same as `Cue.create_job!/1` but the options passed are merged into the options when specifying `use`.

      For example if you had defined:

      ```
      defmodule YourApp do
        use Cue, schedule: "* * * * *"
        ...
      end
      ```

      Then you could override the schedule on a per-job basis with `YourApp.create_job!(name: "some name", schedule: DateTime.utc_now())`.

      The options that can be specified at the "module" level are `name`, `schedule` and `max_retries`.

      The repo used is the one defined in the config.

      Otherwise the behaviour is the same as `Cue.create_job!/1`.
      """
      def create_job!(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo,
          max_retries: unquote(max_retries)
        ]
        |> Keyword.merge(opts)
        |> Cue.create_job!()
      end

      @doc """
      Same things described in `create_job!/1` apply here, but we don't raise, as with `Cue.create_job/1`.
      """
      def create_job(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo,
          max_retries: unquote(max_retries)
        ]
        |> Keyword.merge(opts)
        |> Cue.create_job()
      end

      @doc """
      Same things described in `create_job!/1` apply here, but we don't raise on name conflict, as with `Cue.create_job_unless_exists!/1`.
      """
      def create_job_unless_exists!(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo,
          max_retries: unquote(max_retries)
        ]
        |> Keyword.merge(opts)
        |> Cue.create_job_unless_exists!()
      end

      @doc """
      Same things described in `create_job!/1` apply here, but we don't raise on name conflict, as with `Cue.create_job_unless_exists/1`.
      """
      def create_job_unless_exists(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          schedule: unquote(schedule),
          repo: @repo,
          max_retries: unquote(max_retries)
        ]
        |> Keyword.merge(opts)
        |> Cue.create_job_unless_exists()
      end

      @doc """
      Removes all jobs for this handler. You can filter further by options described in `Cue.remove_jobs/2`.
      """
      def remove_jobs(opts \\ []) do
        callback =
          if function_exported?(__MODULE__, :on_delete, 2) do
            fn job ->
              apply(__MODULE__, :on_delete, [job.name, job.state])
            end
          end

        opts
        |> Keyword.put(:callback, callback)
        |> merge_with_module_defaults()
        |> Cue.remove_jobs()
      end

      @doc """
      Lists all jobs for this handler. Can filter further as described in `Cue.list_jobs/1`.
      """
      def list_jobs(opts \\ []) do
        opts |> merge_with_module_defaults() |> Cue.list_jobs()
      end

      defp merge_with_module_defaults(opts \\ []) do
        default_opts = [repo: @repo, where: [handler: __MODULE__]]

        merged_opts =
          Keyword.merge(default_opts, opts, fn
            # Preserve handler filter
            :where, default, incoming -> Keyword.merge(incoming, default)
            k, v1, v2 -> v2
          end)
      end
    end
  end

  defmodule Error do
    defexception [:message]
  end
end
