defmodule SpiderMan.Component.Downloader do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Response, Pipeline, Utils}

  @impl true
  def handle_message(_processor, message, %{spider: spider} = context) do
    with %{url: url, options: options} = request <-
           Pipeline.call(context.pipelines, message.data, spider),
         {:ok, env} <- context.requester.request(url, options, context),
         %{env: env} <-
           Pipeline.call(context.post_pipelines, %{request: request, env: env}, spider) do
      # push successful events to next_tid
      Utils.push_events_to_next_producer_ets(context.next_tid, context.tid, [
        %Response{key: request.key, env: env, flag: request.flag}
      ])

      %{message | data: :ok}
    else
      :skiped ->
        Message.failed(message, :skiped)

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end
end
