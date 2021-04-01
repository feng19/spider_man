defmodule SpiderMan.Requester do
  @moduledoc false

  @callback request(url :: binary, options :: keyword) :: Tesla.result()
end
