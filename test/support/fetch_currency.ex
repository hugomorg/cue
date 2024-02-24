defmodule Cue.FetchCurrency do
  use Cue, schedule: "* * * * * *"

  require Logger

  def seed_fx_pairs do
    for job <- [[name: "EUR-USD"], [name: "GBP-JPY"], [name: "AUD-SGD", max_retries: 3]] do
      enqueue!(job)
    end
  end

  @impl true
  def init(name) do
    [from_currency, to_currency] = String.split(name, "-", trim: true)
    {:ok, %{from_currency: from_currency, to_currency: to_currency, rate: nil, time: DateTime.utc_now()}}
  end

  @impl true
  def handle_job("EUR-USD", context) do
    # Pretend to fetch forex rates
    :timer.sleep(1000)

    Logger.info("Completed ok for EUR-USD context=#{inspect(context)}")

    {:ok, %{context | rate: :rand.uniform() * 10, time: DateTime.utc_now()}}
  end

  @impl true
  def handle_job("GBP-JPY", context) do
    :timer.sleep(1000)

    Logger.info("Returning error for GBP-JPY context=#{inspect(context)}")

    # Simulate user defined errors
    {:error, "I returned an error"}
  end

  @impl true
  def handle_job("AUD-SGD", _context) do
    :timer.sleep(1000)

    # Simulate unexpected errors
    raise "crashed"
  end

  @impl true
  def handle_job_error(name, context, error_info) do
    Logger.error("Unexpected error name=#{name} context=#{inspect(context)} error_info=#{inspect(error_info, pretty: true, limit: :infinity)}")

    :ok
  end
end
