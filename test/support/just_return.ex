defmodule SpiderMan.Requester.JustReturn do
  @behaviour SpiderMan.Requester
  @impl true
  def request(url, _options, _context) do
    {:ok, %Tesla.Env{url: url}}
  end
end
