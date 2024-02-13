defmodule Cue.MixProject do
  use Mix.Project

  def project do
    [
      app: :cue,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {Cue.Application, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.10"},
      {:postgrex, "~> 0.17", only: [:test]},
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [test: ["ecto.drop", "ecto.create --quiet", "ecto.migrate", "test"]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
