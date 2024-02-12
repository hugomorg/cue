Application.ensure_all_started(:postgrex)
{:ok, _} = Cue.TestRepo.start_link()
ExUnit.start()
