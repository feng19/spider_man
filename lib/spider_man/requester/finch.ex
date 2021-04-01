defmodule SpiderMan.Requester.Finch do
  @moduledoc false
  @behaviour SpiderMan.Requester

  def request(url, options) do
    {adapter_options, options} = Keyword.pop!(options, :adapter_options)
    {middlewares, options} = Keyword.pop!(options, :middlewares)

    middlewares
    |> Tesla.client({Tesla.Adapter.Finch, adapter_options})
    |> Tesla.request([{:url, url} | options])
  end
end
