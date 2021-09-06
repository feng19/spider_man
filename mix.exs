defmodule SpiderMan.MixProject do
  use Mix.Project
  alias SpiderMan.{Component, Requester, Pipeline, Storage}

  @source_url "https://github.com/feng19/spider_man"

  def project do
    [
      app: :spider_man,
      version: "0.4.0",
      elixir: "~> 1.11",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      test_coverage: [ignore_modules: cover_ignore_modules()],
      deps: deps(),
      docs: docs(),
      package: package()
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
      {:broadway, "~> 1.0"},
      {:tesla, "~> 1.4"},
      {:finch, "~> 0.6"},
      {:nimble_options, "~> 0.3"},
      {:jason, "~> 1.2", optional: true},
      {:nimble_csv, "~> 1.1", optional: true},
      {:hackney, "~> 1.7", optional: true},
      {:telemetry_metrics, ">= 0.0.0", optional: true},
      {:logger_file_backend, "~> 0.0.11", optional: true},
      {:ex_doc, ">= 0.0.0", only: [:docs, :dev], runtime: false},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      extras: [
        "LICENSE.md": [title: "License"],
        "README.md": [title: "Overview"]
      ],
      main: "readme",
      source_url: @source_url,
      formatters: ["html"],
      formatter_opts: [gfm: true],
      nest_modules_by_prefix: [Component, Requester, Pipeline, Storage]
    ]
  end

  defp cover_ignore_modules do
    [
      Requester.JustReturn,
      Pipeline.Standard,
      Pipeline.Empty,
      Pipeline.OnlyCall,
      Pipeline.NoCallFunction,
      SpiderMan.Modules,
      SpiderManTest,
      EngineTest,
      Spider0
    ]
  end

  defp package do
    [
      name: "spider_man",
      description: "SpiderMan,a fast high-level web crawling & scraping framework for Elixir",
      files: ["lib", "mix.exs", "README.md", "LICENSE.md"],
      maintainers: ["feng19"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
