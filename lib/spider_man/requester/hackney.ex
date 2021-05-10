defmodule SpiderMan.Requester.Hackney do
  @moduledoc false
  alias SpiderMan.Requester
  @behaviour Requester

  @default_options [
    adapter_options: [recv_timeout: 10_000],
    request_options: [],
    append_default_middlewares?: true,
    middlewares: [],
    logging?: false
  ]

  @impl true
  def request(url, options, %{request_options: request_options, client: client}) do
    options = Map.merge(request_options, options)
    Tesla.request(client, [{:url, url} | options])
  end

  @impl true
  def prepare_for_start(options, downloader_options) do
    options = Keyword.merge(@default_options, options || [])

    client =
      options[:append_default_middlewares?]
      |> Requester.append_default_middlewares(options)
      |> Tesla.client({Tesla.Adapter.Hackney, options[:adapter_options]})

    context = %{
      requester: __MODULE__,
      request_options: options[:request_options],
      client: client
    }

    Keyword.update(downloader_options, :context, context, &Map.merge(&1, context))
  end
end
