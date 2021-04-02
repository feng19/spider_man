defmodule SpiderMan.ItemProcessor do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.Pipeline

  @impl true
  def handle_message(_processor, message, context) do
    data = message.data

    if context[:debug] do
      Logger.debug("ItemProcessor get message: #{inspect(data)}")
    end

    case Pipeline.pipe(context.pipelines, data) do
      :skiped -> Message.failed(message, :skiped)
      {:error, reason} -> Message.failed(message, reason)
      {batcher, item} -> %{message | data: item, batcher: batcher}
      item -> %{message | data: item}
    end
  end

  @impl true
  def handle_batch(batcher, messages, _batch_info, %{
        storage: storage,
        storage_options: storage_options
      }) do
    items = Stream.map(messages, & &1.data)

    storage.store(batcher, items, storage_options)
    |> Stream.zip(messages)
    |> Enum.map(fn
      {:ok, message} -> message
      {{:error, reason}, message} -> Message.failed(message, reason)
    end)
  end
end
