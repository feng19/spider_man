defmodule SpiderMan.Storage.Log do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Storage

  @impl true
  def store(_batcher, items, _context) do
    Enum.map(items, fn item ->
      Logger.info(">> store item: #{inspect(item)}")
      :ok
    end)
  end
end
