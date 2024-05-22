defmodule Cue.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Cue.DataCase

      alias Cue.TestRepo
      alias Cue.Schemas.Person
    end
  end

  setup tags do
    start_supervised(Cue.TestRepo)
    Cue.DataCase.setup_sandbox(tags)
    :ok
  end

  @doc """
  Sets up the sandbox based on the test tags.
  """
  def setup_sandbox(tags) do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Cue.TestRepo, shared: not tags[:async])

    on_exit(fn ->
      Ecto.Adapters.SQL.Sandbox.stop_owner(pid)
    end)
  end

  def agent(context) do
    agent =
      start_supervised!({Agent, fn -> Agent.start_link(fn -> %{} end, name: context.test) end})

    %{agent: agent}
  end
end
