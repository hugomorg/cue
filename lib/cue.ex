defmodule Cue do
  @moduledoc """
  Documentation for `Cue`.
  """

  @type name :: String.t()
  @type context :: any()
  @callback init(name) :: {:ok, any()}
  @callback handle_job(name, context) :: :ok | {:ok, map()} | {:error, any()}
  @type error_info :: %{
          error: String.t(),
          retry_count: non_neg_integer(),
          max_retries: non_neg_integer()
        }
  @callback handle_job_error(name, context, error_info) :: :ok | {:ok, map()} | {:error, any()}

  @optional_callbacks [init: 1]

  alias Cue.Schemas.Job

  def enqueue!(opts) do
    opts =
      Keyword.validate!(opts, [
        :handler,
        :repo,
        :name,
        :schedule,
        :error_handler,
        run_now: false,
        one_off: false,
        max_retries: nil,
        context: nil
      ])

    handler = {module, _function} = validate_job_handler!(opts[:handler])
    error_handler = validate_error_handler!(opts[:error_handler])

    context = init_job(opts[:name], module)

    run_at =
      if opts[:run_now] do
        DateTime.utc_now()
      else
        Job.next_run_at!(opts[:schedule])
      end

    %Job{}
    |> Job.changeset(%{
      name: opts[:name],
      handler: handler,
      error_handler: error_handler,
      run_at: run_at,
      schedule: opts[:schedule],
      status: :not_started,
      max_retries: opts[:max_retries],
      one_off: opts[:one_off],
      context: context
    })
    |> opts[:repo].insert!()
  end

  def dequeue(repo, job_name) do
    require Ecto.Query

    # Synchronously remove job from scheduling/processing
    Cue.Scheduler.add_job_to_ignored(job_name)
    Cue.Processor.remove_job(job_name)

    {count, _returned} =
      Job
      |> Ecto.Query.where(name: ^job_name)
      |> repo.delete_all()

    Cue.Scheduler.remove_job_from_ignored(job_name)

    count
  end

  defp validate_job_handler!(handler) when is_atom(handler) do
    validate_handler!(
      {handler, :handle_job},
      2,
      &"Error handling module #{inspect(&1)} doesn't exist",
      &"Function #{inspect(&2)} given as error handling function in module #{&1} but it doesn't exist"
    )
  end

  defp validate_error_handler!(handler) do
    validate_handler!(
      {handler, :handle_job_error},
      3,
      &"Job handling module #{inspect(&1)} doesn't exist",
      &"Function #{inspect(&2)} given as job handling function in module #{&1} but it doesn't exist"
    )
  end

  defp validate_handler!({module, fun}, arity, no_module_error, no_function_error)
       when is_atom(module) and is_atom(fun) do
    module_exists? = Code.ensure_loaded?(module)

    cond do
      module_exists? and function_exported?(module, fun, arity) -> {module, fun}
      module_exists? -> raise "#{no_function_error.(module, fun)}"
      :else -> raise "#{no_module_error.(module)}"
    end
  end

  defp init_job(name, module) do
    if function_exported?(module, :init, 1) do
      case module.init(name) do
        {:ok, context} ->
          context

        unexpected_return ->
          raise "You must return `{:ok, context}` from your `init/1` function, received #{inspect(unexpected_return)}"
      end
    end
  end

  defmacro __using__(opts) do
    name = Keyword.get(opts, :name)
    schedule = Keyword.fetch!(opts, :schedule)

    quote do
      @behaviour Cue
      @repo Application.compile_env!(:cue, :repo)
      @cue_name unquote(name) || String.replace("#{__MODULE__}", ~r/^Elixir\./, "")

      def enqueue!(opts \\ []) do
        [
          handler: __MODULE__,
          name: @cue_name,
          error_handler: __MODULE__,
          schedule: unquote(schedule),
          repo: @repo
        ]
        |> Keyword.merge(opts)
        |> Cue.enqueue!()
      end

      def dequeue do
        Cue.dequeue(@repo, @cue_name)
      end
    end
  end
end
