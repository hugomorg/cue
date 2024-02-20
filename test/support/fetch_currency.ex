defmodule Cue.FetchCurrency do
  use Cue, name: "fetch_currency", schedule: "* * * * * *"

  @impl true
  def handle_job(last_job) do
    # pretend to fetch forex rates
    :timer.sleep(3000)

    IO.inspect(last_job, label: "handle_job")

    case :rand.uniform() do
      n when n < 0.2 ->
        raise "me up"

      n when n < 0.4 ->
        {:error, "failed b/c"}

      _ ->
        {:ok, %{id: System.unique_integer([:positive, :monotonic])}}
    end
  end

  @impl true
  def handle_job_error(last_job) do
    IO.inspect(last_job, label: "handle_job_error")

    :ok
  end
end
