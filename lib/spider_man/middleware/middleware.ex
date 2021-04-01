defmodule SpiderMan.Middleware do
  @moduledoc false

  @callback call(event, arg) :: {:ok, event} | {:error, error} | event
            when event: term, arg: term, error: term
  @callback prepare_for_start(arg, options) :: arg when arg: term, options: keyword
  @callback prepare_for_stop(arg :: term) :: :ok
  @optional_callbacks call: 2, prepare_for_start: 2, prepare_for_stop: 1
end
