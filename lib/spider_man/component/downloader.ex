defmodule SpiderMan.Component.Downloader do
  @moduledoc """
  Download request.

  Life cycle of request:
    0. insert requests to ets of downloader component.
    1. component's producer get pass out to processes.
    2. processes handle message.
      1. handle by pre pipelines.
      2. call `Requester.request/3`.
      3. handle by post pipelines.
    3. pass out message.
      1. if success, pass out to Spider component's ets.
      2. if failed, maybe try again.
  """
  use SpiderMan.Component, name: :downloader
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Component, Pipeline, Response}

  @event_name [:spider_man, :downloader, :stop]

  @impl true
  def handle_message(_processor, message, %{spider: spider, pipelines: pipelines} = context) do
    start_time = System.monotonic_time()
    metadata = %{name: inspect(spider)}
    Logger.metadata(spider: spider)

    with %{url: url, options: options} = request <- Pipeline.call(pipelines, message.data),
         {:ok, env} <- context.requester.request(url, options, context),
         %{env: env} <- Pipeline.call(context.post_pipelines, %{request: request, env: env}) do
      duration = System.monotonic_time() - start_time
      :telemetry.execute(@event_name, %{success: 1, fail: 0, duration: duration}, metadata)

      response = %Response{key: request.key, env: env, flag: request.flag}
      Component.push_to_next_component(context, [response])
      :telemetry.execute([:spider_man, :spider, :start], %{count: 1}, metadata)
      %{message | data: :ok}
    else
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
end
