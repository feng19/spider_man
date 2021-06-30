defmodule SpiderMan.Storage.Multi do
  @moduledoc false
  @behaviour SpiderMan.Storage

  @impl true
  def store(batcher, items, %{storage_list: storage_list}) do
    Enum.each(storage_list, fn %{storage: storage, storage_context: storage_context} ->
      case storage.store(batcher, items, storage_context) do
        :ok ->
          :ok

        list when is_list(list) ->
          true = Enum.all?(list, &match?(:ok, &1))
      end
    end)
  end
end
