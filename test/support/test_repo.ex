defmodule Cue.TestRepo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :cue,
    adapter: Ecto.Adapters.Postgres
end
