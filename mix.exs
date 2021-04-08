defmodule SpiderMan.MixProject do
  use Mix.Project

  def project do
    [
      app: :spider_man,
      version: "0.1.0",
      elixir: "~> 1.11",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {SpiderMan.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 0.6"},
      {:tesla, "~> 1.4"},
      {:finch, "~> 0.6"},
      {:jason, "~> 1.2", optional: true},
      {:telemetry_metrics, "~> 0.6.0", optional: true}
    ]
  end
end
