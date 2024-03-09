defmodule Cue.MixProject do
  use Mix.Project

  def project do
    [
      app: :cue,
      version: "0.0.1",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      description: "Postgres backed queue and scheduler.",
      package: [
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/hugomorg/cue"}
      ],
      source_url: "https://github.com/hugomorg/cue"
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.10"},
      {:postgrex, "~> 0.17", only: [:test, :dev]},
      {:jason, "~> 1.4"},
      {:cron, "~> 0.1"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [test: ["ecto.drop", "ecto.create --quiet", "ecto.migrate", "test"]]
  end

  defp elixirc_paths(env) when env in [:dev, :test], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
