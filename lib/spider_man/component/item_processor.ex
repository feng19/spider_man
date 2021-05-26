defmodule SpiderMan.Component.ItemProcessor do
  @moduledoc false
  use SpiderMan.Component
  require Logger
  alias Broadway.Message
  alias SpiderMan.Pipeline

  @impl true
  def handle_message(_processor, message, %{spider: spider, pipelines: pipelines}) do
    Logger.metadata(spider: spider)

    case Pipeline.call(pipelines, message.data) do
      :skiped -> Message.failed(message, :skiped)
      {:error, reason} -> Message.failed(message, reason)
      {batcher, item} -> %{message | data: item, batcher: batcher}
      item -> %{message | data: item}
    end
  end

  @impl true
  def handle_batch(batcher, messages, _batch_info, %{
        spider: spider,
        storage: storage,
        storage_context: storage_context
      }) do
    Logger.metadata(spider: spider)
    items = Stream.map(messages, & &1.data)

    case storage.store(batcher, items, storage_context) do
      :ok ->
        messages

      {:error, reason} ->
        Enum.map(messages, &Message.failed(&1, reason))

      enumerable ->
        enumerable
        |> Stream.zip(messages)
        |> Enum.map(fn
          {:ok, message} -> message
          {{:error, reason}, message} -> Message.failed(message, reason)
        end)
    end
  end
end
