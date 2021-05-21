defmodule SpiderMan.Component.Spider do
  @moduledoc false
  use SpiderMan.Component
  require Logger
  alias Broadway.Message
  alias SpiderMan.{Pipeline, Component}

  @impl true
  def handle_message(_processor, message, %{spider: spider, pipelines: pipelines} = context) do
    case Pipeline.call(pipelines, message.data, spider) do
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
            Component.push_to_next_component(context, items)

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
