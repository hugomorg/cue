defmodule CryptoFundingTracker.Handler do
  @moduledoc false
  use Ecto.Type

  @impl true
  def type(), do: :handler

  @impl true
  def cast(_), do: :error

  @impl true
  def dump(module) when is_atom(module) do
    {:ok, :erlang.term_to_binary(module)}
  end

  def dump({module, function}) when is_atom(module) and is_atom(function) do
    {:ok, :erlang.term_to_binary({module, function})}
  end

  def dump(_), do: :error

  @impl true
  def load(handler) when is_binary(handler) do
    {:ok, :erlang.binary_to_term(handler)}
  end

  def load(_), do: :error
end
