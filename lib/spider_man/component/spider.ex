defmodule SpiderMan.Spider do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.Utils

  @impl true
  def handle_message(_processor, message, context) do
    Logger.debug("Spider get message: #{inspect(message.data)}")

    case Enum.reduce_while(context.middlewares, message.data, &Utils.pipe/2) do
      %{env: env, options: options} ->
        spider = context.spider

        options =
          context
          |> Map.to_list()
          |> Keyword.merge(options)

        case spider.handle_response(env, options) do
          return when is_map(return) ->
            case Map.get(return, :requests, []) do
              [] ->
                :skip

              requests when is_list(requests) ->
                objects = Enum.map(requests, &{&1.key, &1})

                options
                |> Keyword.fetch!(:prev_tid)
                |> :ets.insert(objects)
            end

            items = Map.get(return, :items, [])
            %{message | data: items}

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
