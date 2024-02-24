defmodule Cue.ElixirTerm do
  @moduledoc false
  use Ecto.Type

  @impl true
  def type(), do: :elixir_term

  @impl true
  def cast(elixir_term), do: {:ok, elixir_term}

  @impl true
  def dump(elixir_term) do
    {:ok, :erlang.term_to_binary(elixir_term)}
  end

  @impl true
  def load(elixir_term) when is_binary(elixir_term) do
    {:ok, :erlang.binary_to_term(elixir_term)}
  end

  def load(_), do: :error
end
