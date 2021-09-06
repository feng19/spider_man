defmodule SpiderMan.Component.Spider do
  @moduledoc """
  Analyze web pages.

  Life cycle of request:
    0. insert responses to ets of spider component.
    1. component's producer get pass out to processes.
    2. processes handle message.
      1. handle by pre pipelines.
      2. call `SpiderModule.handle_response/2`.
    3. pass out message.
      1. if success, pass out requests to Downloader component's ets.
      1. if success, pass out items to ItemProcessor component's ets.
      2. if failed, maybe try again.
  """
  use SpiderMan.Component, name: :spider
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Component, Pipeline}

  @event_name [:spider_man, :spider, :stop]

  @impl true
  def handle_message(_processor, message, %{spider: spider, pipelines: pipelines} = context) do
    start_time = System.monotonic_time()
    metadata = %{name: inspect(spider)}
    Logger.metadata(spider: spider)

    case Pipeline.call(pipelines, message.data) do
      response when is_struct(response) ->
        handle_response(response, message, context, start_time, metadata)

      :skiped ->
        duration = System.monotonic_time() - start_time
        :telemetry.execute(@event_name, %{success: 1, fail: 0, duration: duration}, metadata)
        Message.failed(message, :skiped)

      {:error, reason} ->
        duration = System.monotonic_time() - start_time
        :telemetry.execute(@event_name, %{success: 0, fail: 1, duration: duration}, metadata)
        Message.failed(message, reason)
    end
  end

  defp handle_response(
         response,
         message,
         %{spider_module: spider_module} = context,
         start_time,
         metadata
       ) do
    case spider_module.handle_response(response, context) do
      return when is_map(return) ->
        duration = System.monotonic_time() - start_time
        :telemetry.execute(@event_name, %{success: 1, fail: 0, duration: duration}, metadata)

        case Map.get(return, :requests, []) do
          [] ->
            :skip

          requests when is_list(requests) ->
            objects = Enum.map(requests, &{&1.key, &1})

            :telemetry.execute(
              [:spider_man, :downloader, :start],
              %{count: length(objects)},
              metadata
            )

            :ets.insert(response.options[:prev_tid], objects)
        end

        items = Map.get(return, :items, [])
        Component.push_to_next_component(context, items)

        :telemetry.execute(
          [:spider_man, :item_processor, :start],
          %{count: length(items)},
          metadata
        )

        %{message | data: :ok}

      {:error, reason} ->
        duration = System.monotonic_time() - start_time
        :telemetry.execute(@event_name, %{success: 0, fail: 1, duration: duration}, metadata)
        Message.failed(message, reason)
    end
  end
end
