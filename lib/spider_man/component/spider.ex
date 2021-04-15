defmodule SpiderMan.Spider do
  @moduledoc false
  use SpiderMan.Component.Builder
  require Logger
  alias Broadway.Message
  alias SpiderMan.Pipeline

  @impl true
  def handle_message(_processor, message, context) do
    data = message.data

    if context[:debug] do
      Logger.debug("Spider get message: #{inspect(data)}")
    end

    case Pipeline.pipe(context.pipelines, data) do
      response when is_struct(response) ->
        spider_module = context.spider_module

        case spider_module.handle_response(response, context) do
          return when is_map(return) ->
            case Map.get(return, :requests, []) do
              [] ->
                :skip

              requests when is_list(requests) ->
                objects = Enum.map(requests, &{&1.key, &1})
                :ets.insert(response.options[:prev_tid], objects)
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
