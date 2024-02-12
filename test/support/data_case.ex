defmodule Cue.DataCase do
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  using do
    quote do
      import Ecto
      import Ecto.Changeset
      import Ecto.Query

      alias Cue.TestRepo
      alias Cue.Schemas.Person
    end
  end

  setup tags do
    :ok = Sandbox.checkout(Cue.TestRepo)

    unless tags[:async] do
      Sandbox.mode(Cue.TestRepo, {:shared, self()})
    end

    :ok
  end
end
