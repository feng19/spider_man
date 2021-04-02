defmodule SpiderMan.Requester.Finch do
  @moduledoc false
  @behaviour SpiderMan.Requester

  @impl true
  def request(url, options, context) do
    options = Keyword.merge(context.request_options, options)

    context.middlewares
    |> Tesla.client({Tesla.Adapter.Finch, context.adapter_options})
    |> Tesla.request([{:url, url} | options])
  end
end
