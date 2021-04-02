defmodule SpiderMan.Requester do
  @moduledoc false

  @callback request(url :: binary, options :: [Tesla.option()], context :: map) :: Tesla.result()
end
