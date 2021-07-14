defmodule SpiderMan.Component.ItemProcessor do
  @moduledoc false
  use SpiderMan.Component
  require Logger
  alias Broadway.Message
  alias SpiderMan.Pipeline

  @event_name [:spider_man, :item_processor, :stop]

  @impl true
  def handle_message(_processor, message, %{spider: spider, pipelines: pipelines}) do
    start_time = System.monotonic_time()
    Logger.metadata(spider: spider)

    case Pipeline.call(pipelines, message.data) do
      :skiped ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(@event_name, %{success: 1, fail: 0, duration: duration}, %{
          spider: inspect(spider)
        })

        Message.failed(message, :skiped)

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(@event_name, %{success: 0, fail: 1, duration: duration}, %{
          spider: inspect(spider)
        })

        Message.failed(message, reason)

      {batcher, item} ->
        %{message | data: item, batcher: batcher}

      item ->
        %{message | data: item}
    end
  end

  @impl true
  def handle_batch(batcher, messages, _batch_info, %{
        spider: spider,
        storage: storage,
        storage_context: storage_context
      }) do
    start_time = System.monotonic_time()
    metadata = %{name: inspect(spider)}
    Logger.metadata(spider: spider)
    items = Stream.map(messages, & &1.data)

    case storage.store(batcher, items, storage_context) do
      :ok ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          @event_name,
          %{success: length(messages), fail: 0, duration: duration},
          metadata
        )

        messages

      {:error, reason} ->
        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          @event_name,
          %{success: 0, fail: length(messages), duration: duration},
          metadata
        )

        Enum.map(messages, &Message.failed(&1, reason))

      enumerable ->
        duration = System.monotonic_time() - start_time

        {messages, {success, fail}} =
          enumerable
          |> Stream.zip(messages)
          |> Enum.map_reduce({0, 0}, fn
            {:ok, message}, {s, f} -> {message, {s + 1, f}}
            {{:error, reason}, message}, {s, f} -> {Message.failed(message, reason), {s, f + 1}}
          end)

        :telemetry.execute(
          @event_name,
          %{success: success, fail: fail, duration: duration},
          metadata
        )

        messages
    end
  end
end
