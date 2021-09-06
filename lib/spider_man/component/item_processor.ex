defmodule SpiderMan.Component.ItemProcessor do
  @moduledoc """
  Store items.

  Life cycle of request:
    0. insert requests to ets of downloader.
    1. downloader's producer get pass out to processes.
    2. processes handle message.
      1. handle by pre pipelines.
      2. call `Requester.request/3`.
      3. handle by post pipelines.
    3. pass out message.
      1. if success, pass out to Batchers.
      2. if failed, maybe try again.
    4. batcher get enough message and call `handle_batch/4`.
      1. call `Storage.store/3` to save items.
      2. if success, done.
      3. if failed, maybe try again.
  """
  use SpiderMan.Component, name: :item_processor
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
