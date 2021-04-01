defmodule SpiderMan.Downloader do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Response, Utils}

  @impl true
  def handle_message(_processor, message, context) do
    Logger.debug("Downloader get message: #{inspect(message.data)}")

    case Enum.reduce_while(context.middlewares, message.data, &Utils.pipe/2) do
      %{url: url, options: options} ->
        request_options = Keyword.merge(context.request_options, options)
        requester = context.requester

        case requester.request(url, request_options) do
          {:ok, env} ->
            %{message | data: %Response{key: url, env: env}}

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
