defmodule SpiderMan.Storage.Log do
  @moduledoc """
  Just log each item by Logger

  ## Usage
  ```elixir
  settings = [
    ...
    item_processor_options: [
      storage: [#{inspect(__MODULE__)}]
    ]
  ]
  ```
  """
  require Logger
  @behaviour SpiderMan.Storage

  @impl true
  def store(_batcher, items, _context) do
    Enum.each(items, fn item ->
      Logger.info(">> store item: #{inspect(item)}")
    end)
  end
end
