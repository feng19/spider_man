defmodule SpiderMan.Pipeline.Splash do
  @moduledoc false
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(event, {api_url, query}) do
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
    query = Keyword.merge([{:url, url} | query], splash_options)
    options = Keyword.put(options, :query, query)
    %{event | url: api_url, options: options}
  end

  @impl true
  def prepare_for_start(query, options) do
    {Keyword.pop(query || [], :api_url, "http://localhost:8050/render.html"), options}
  end
end
