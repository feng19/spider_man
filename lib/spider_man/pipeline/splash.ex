defmodule SpiderMan.Pipeline.Splash do
  @default_url "http://localhost:8050/render.html"

  @moduledoc """
  use [Splash](https://splash.readthedocs.io/en/stable/) for javascript rendering service

  ## Usage
  ```elixir
  settings = [
    ...
    downloader_options: [
      pipelines: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, %{url: #{inspect(@default_url)}, query: query_for_splash}}]
    ]
  ]
  ```

  If didn't set dir for this pipeline, the default is `%{url: #{inspect(@default_url)}, query: []}`.
  """
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(event, opts) do
    %{url: api_url, query: query_for_splash} = Map.new(opts)
    options = event.options

    url =
      options
      |> Keyword.get(:query, [])
      |> Keyword.delete(:method)
      |> URI.encode_query()
      |> case do
        "" -> event.url
        q -> event.url <> "?" <> q
      end

    splash_options = Keyword.get(options, :splash_options, method: :get)
    query = Keyword.merge([{:url, url} | query_for_splash], splash_options)
    options = Keyword.put(options, :query, query)
    %{event | url: api_url, options: options}
  end

  @impl true
  def prepare_for_start(nil, options), do: {%{url: @default_url, query: []}, options}
  def prepare_for_start(opts, options) when is_list(opts), do: {Map.new(opts), options}
  def prepare_for_start(opts, options) when is_map(opts), do: {opts, options}
end
