defmodule SpiderMan.Component.Downloader do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Response, Pipeline, Utils}

  @impl true
  def handle_message(_processor, message, %{spider: spider} = context) do
    data = message.data

    if context[:debug] do
      Logger.debug("Downloader get message: #{inspect(data)}", spider: spider)
    end

    case Pipeline.call(context.pipelines, data, spider) do
      %{key: key, url: url, options: options, flag: flag} ->
        requester = context.requester

        case requester.request(url, options, context) do
          {:ok, env} ->
            # push successful events to next_tid
            Utils.push_events_to_next_producer_ets(context.next_tid, context.tid, [
              %Response{key: key, env: env, flag: flag}
            ])

            %{message | data: :ok}

          {:error, reason} ->
            Message.failed(message, reason)
        end

      :skiped ->
        Message.failed(message, :skiped)

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end
end
