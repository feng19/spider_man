defmodule SpiderMan.Storage do
  @moduledoc false

  @callback store(batcher :: atom, items :: Enumerable.t(), storage_options :: keyword) ::
              [:ok] | [{:error, term}]
end
