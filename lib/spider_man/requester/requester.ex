defmodule SpiderMan.Requester do
  @moduledoc false

  @callback request(url :: binary, options :: [Tesla.option()], context :: map) :: Tesla.result()
  @callback prepare_for_start(arg :: term, options) :: options when options: keyword
  @callback prepare_for_stop(options :: keyword) :: :ok
  @optional_callbacks prepare_for_start: 2, prepare_for_stop: 1
end
