# Cue

Cue helps you schedule and run jobs. It uses Postgres as a database backend and assumes you are using [`ecto`](https://hexdocs.pm/ecto/Ecto.html).

The first thing you need to do is run a migration with `ecto.gen.migration` so that we can persist the jobs. You can name this migration whatever you wish.

Then simply paste this into the migration module (the module name will probably be different, as it depends on the name you gave to `ecto.gen.migration`):

```elixir
defmodule YourApp.CreateCueJobsTable do
  use Cue.Migration
end
```

Don't forget to run `mix ecto.migrate`!

This will create a table `cue_jobs` and some indexes.

You will need specify your `Repo` module:

```elixir
config :cue, repo: MyApp.Repo
```

You will also want to start `Cue` under your supervision tree, after the `Repo` you just specified. For example:

```elixir
defmodule YourApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [YourApp.Repo, Cue]

    opts = [strategy: :one_for_one, name: YourApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

Or to give it a spin, run `Cue.start_link`.

The next step is scoping the job to a module. As a concrete example, let's assume you have a weather app which needs the latest weather. You want to be quite up-to-date so you decide to call it every minute. In other words, you want to run a job which is scheduled to repeat every minute.

A schedule can be either a [cron specification](https://crontab.guru/) or a UTC `DateTime` value (see more below).

You can use `Cue` like this:

```elixir
defmodule YourApp do
  use Cue, schedule: "* * * * *"

  @impl true
  def handle_job(name, state) do
    # Business logic goes here...
    WeatherAPI.list()
    :ok
  end

  @impl true
  def handle_job_error(name, state, error_info) do
    # Do something with the error
    :ok
  end
end
```

Just one final step: when your app is running, call `YourApp.create_job!()`.

This creates the job, and everything else will be taken care of!

If you don't specify a name, the module is used as the default name. But names must be unique.

## One module, multiple jobs

Let's look at a slightly more complex example. You want to fetch the weather across different continents (let's say London and New York), so there are different APIs. However, you still want to group them under one module.

No problem: just call `YourApp.create_job!(name: city)` where `city` is the unique name you choose for the job. Then you can just pattern match when the job is handled.

If you have some code which automatically creates jobs, then a useful alternative is `YourApp.create_job_unless_exists/1` / `YourApp.create_job_unless_exists!/1` which creates the job but does nothing if it exists, instead of causing an error.

```elixir
defmodule YourApp do
  use Cue, schedule: "* * * * *"

  @impl true
  def handle_job("New York", state) do
    USWeatherAPI.list()
    # More business logic goes here...
    :ok
  end

  @impl true
  def handle_job("London", state) do
    UKWeatherAPI.list()
    # More business logic goes here...
    :ok
  end

  @impl true
  def handle_job_error(name, state, error_info) do
    # Do something with the error

    :ok
  end
end
```

## One-off jobs

Ok, so now we know how to schedule jobs. But what about one-off jobs? If the weather is really bad, maybe you want to send an email. But you don't want this to repeat - just ensure it is handled properly within a certain time-frame.

Simply pass a UTC `DateTime` as the `schedule` in `create_job!/1` / `create_job/1`. If you want the job to run immediately you can do something like `YourApp.create_job!(schedule: DateTime.utc_now())` (don't worry if it is in the past, it will still run immediately).

This will not repeat, even if it fails.

## Deleting jobs

If you want to remove a single job, it's: `YourApp.remove_job(name)`.

You can also remove all jobs that the module handles with `YourApp.remove_jobs()`.

You can filter more precisely, for example `YourApp.remove_jobs(where: [name: [ilike: "job%"]])`. See `Cue.remove_jobs/1` for more info.

If you define an `on_delete/2` handler in your module, this will get called just before the jobs are deleted, if you need to do any cleanup. This runs before any job is deleted, so if there is a crash no jobs will get deleted.

## How can I see all the jobs?

To see jobs scoped to a module, call `YourApp.list_jobs()`.

Both of these take some filtering options. Say you wanted to find only failed jobs that failed before a certain time. You could do this:

```elixir
YourModule.list_jobs(where: [status: :failed], [failed_at: [<: some_date_time]])
```

For more global searches and options, see `Cue.list_jobs/1`.

## Keeping context / state

OK, next problem. You are fetching the weather every minute, and you want to keep a running average of the rainfall.

In other words, you want to keep "context" or "state". Doing this with `cue` is simple. Just return `{:ok, {:state, next_state}}` from the job handler, where `next_state` is the entire state you want the job to have. The handler will receive the latest state on the next run.

Apart from returning error tuples (see below), anything else returned from the job handler will not be treated in a special way.

If you want to avoid a `nil` state on the first `handle_job/2` call or have some expensive setup, just define a callback `init/1` in your module. This will get called with the `name` of the job. It gets run once, when the job is created, and is used to initialise the state for that particular job.

So the return value from `init/1` gets passed into the first `handle_job/2` call.

`state` can be any Elixir expression. Obviously if you are limited in terms of disk or memory or just have a lot of jobs you may want to be careful about storing too much here.

```elixir
defmodule YourApp do
  use Cue, schedule: "* * * * *"

  @impl true
  def init(name) do
    {:ok, %{data: [], averages: [], window_size: 5}}
  end

  @impl true
  def handle_job(name, state) do
    latest_rainfall = WeatherAPI.get_rainfall()
    next_data = Enum.take([latest_rainfall | state.data], state.window_size)
    average = Enum.sum(next_data) / length(next_data)
    {:ok, %{state | data: next_data, averages: [average | state.averages]}}
  end

  @impl true
  def handle_job_error(name, state, error_info) do
    # Do something with the error

    :ok
  end
end
```

## Error handling

In the context of `Cue` there are two types of errors: crashes and errors returned by you.

If there is a crash, it will get caught, and your error handler `handle_job_error/3` will be invoked. The job is marked as failed. If you return an `{error, reason}` tuple from `handle_job/2`, exactly the same thing will happen.

The 3rd argument of `handle_job_error/3` is a map containing `error` (the error reason), `retry_count` and `max_retries`. By default `max_retries` is `nil`, meaning the job will retry infinitely. By retry, we mean that it will get re-run at the next scheduled time. One-off jobs will not get re-run.

You can define `max_retries` at the module level, and also override it in `create_job!/1`/`create_job/1`. If the `max_retries` is exceeded then the job is paused.

If you want to update state after you return an error, you can do so by returning `{:error, error, {:state, next_state}}`.

```elixir
defmodule YourApp do
  use Cue, schedule: "* * * * *", max_retries: 2

  @impl true
  def handle_job(name, state) do
    case API.call() do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def handle_job_error(name, state, error_info) do
    # Do something with the error

    :ok
  end
end
```

## Concurrency and timeouts

Under the hood, each job is run as an Elixir [`Task`](https://hexdocs.pm/elixir/Task.html). You can specify the `timeout` in your config (in milliseconds). Default is 5000 ms, or 5 seconds.

Concurrency is also configurable with `max_concurrency`. This is important to consider in case you have a lot of jobs scheduled to run at the same time, but your database connections are relatively limited. In such a case, you might want to start it with a small number, and increase it. The default is `5`.

```elixir
# Changing the defaults
config :cue, timeout: 10_000, max_concurrency: 10
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `cue` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:cue, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/cue>.

