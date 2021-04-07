defmodule SpiderMan.Storage do
  @moduledoc false

  @callback store(batcher :: atom, items :: Enumerable.t(), storage_options :: keyword) ::
              [:ok] | [{:error, term}]
  @callback prepare_for_start(arg :: term, options) :: options when options: keyword
  @callback prepare_for_stop(options :: keyword) :: :ok
  @optional_callbacks prepare_for_start: 2, prepare_for_stop: 1
end
