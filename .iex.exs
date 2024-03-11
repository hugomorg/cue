start = fn ->
  {:ok, _} = Cue.TestRepo.start_link()
  Cue.start_link([])
  Cue.FetchCurrency.seed_fx_pairs()
end
