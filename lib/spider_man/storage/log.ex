defmodule SpiderMan.Storage.Log do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Storage

  @impl true
  def store(_, items, %{spider: spider}) do
    Enum.map(items, fn item ->
      Logger.info(">> store item: #{inspect(item)}", spider: spider)
      :ok
    end)
  end
end
