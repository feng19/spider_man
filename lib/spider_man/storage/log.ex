defmodule SpiderMan.Storage.Log do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Storage

  @impl true
  def store(_batcher, items, _context) do
    Enum.each(items, fn item ->
      Logger.info(">> store item: #{inspect(item)}")
    end)
  end
end
