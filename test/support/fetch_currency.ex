defmodule Cue.FetchCurrency do
  use Cue, schedule: "* * * * * *"

  require Logger

  def seed_fx_pairs do
    special_pairs = [
      [name: "EUR-USD"],
      [name: "GBP-JPY", max_retries: 2],
      [name: "AUD-SGD", max_retries: 3]
    ]

    combos =
      for(char_1 <- ?A..?Z, char_2 <- ?A..?Z, do: [name: "#{<<char_1>>}-#{<<char_2>>}"])
      |> Enum.shuffle()
      |> Enum.take(100)

    for job <- special_pairs ++ combos do
      create_job!(job)
    end
  end

  @impl true
  def init(name) do
    [from_currency, to_currency] = String.split(name, "-", trim: true)

    {:ok,
     %{
       from_currency: from_currency,
       to_currency: to_currency,
       rate: nil,
       time: DateTime.utc_now()
     }}
  end

  @impl true
  def handle_job("EUR-USD", state) do
    # Pretend to fetch forex rates
    :timer.sleep(1000)

    Logger.info("Completed ok for EUR-USD state=#{inspect(state)}")

    {:ok, %{state | rate: :rand.uniform() * 10, time: DateTime.utc_now()}}
  end

  @impl true
  def handle_job("GBP-JPY", state) do
    :timer.sleep(1000)

    Logger.info("Returning error for GBP-JPY state=#{inspect(state)}")

    # Simulate user defined errors
    {:error, "I returned an error"}
  end

  @impl true
  def handle_job("AUD-SGD", _state) do
    :timer.sleep(1000)

    # Simulate unexpected errors
    raise "crashed"
  end

  @impl true
  def handle_job(name, _state) do
    :timer.sleep(trunc(:rand.uniform() * 2000))

    Logger.info("Handling name=#{name}")

    :ok
  end

  @impl true
  def handle_job_error(name, state, error_info) do
    Logger.error(
      "Unexpected error name=#{name} state=#{inspect(state)} error_info=#{inspect(error_info, pretty: true, limit: :infinity)}"
    )

    :ok
  end
end
