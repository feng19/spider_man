defmodule SpiderMan.Component.ItemProcessor do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.Pipeline

  @impl true
  def handle_message(_processor, message, %{spider: spider} = context) do
    case Pipeline.call(context.pipelines, message.data, spider) do
      :skiped -> Message.failed(message, :skiped)
      {:error, reason} -> Message.failed(message, reason)
      {batcher, item} -> %{message | data: item, batcher: batcher}
      item -> %{message | data: item}
    end
  end

  @impl true
  def handle_batch(batcher, messages, _batch_info, %{
        storage: storage,
        storage_context: storage_context
      }) do
    items = Stream.map(messages, & &1.data)

    storage.store(batcher, items, storage_context)
    |> Stream.zip(messages)
    |> Enum.map(fn
      {:ok, message} -> message
      {{:error, reason}, message} -> Message.failed(message, reason)
    end)
  end
end
