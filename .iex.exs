if Code.ensure_loaded?(Cue.FetchCurrency) do
  if Mix.env() == :test do
    alias Cue.TestRepo
    alias Cue.Schemas.Job

    TestRepo.insert!(%Job{
      name: "fetch_fx",
      handler: :erlang.term_to_binary(Cue.FetchCurrency),
      run_at: DateTime.utc_now() |> DateTime.add(2) |> DateTime.truncate(:second),
      interval: 5,
      status: :not_started,
    }, on_conflict: {:replace, [:run_at, :status]}, conflict_target: :name)
  end
end
