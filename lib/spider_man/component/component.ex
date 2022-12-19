defmodule SpiderMan.Component do
  @moduledoc false

  defmacro __using__(opts \\ []) do
    quote location: :keep do
      use Broadway
      require Logger

      def start_link(options) do
        processor = Keyword.get(options, :processor, [])
        component = unquote(Keyword.fetch!(opts, :name))

        Broadway.start_link(__MODULE__,
          name: SpiderMan.Producer.process_name(options[:spider], component),
          producer: options[:producer],
          processors: [default: processor],
          batchers: Keyword.get(options, :batchers, []),
          context: options[:context]
        )
      end

      @impl true
      def process_name({:via, Registry, {SpiderMan.Registry, {spider, component}}}, base_name) do
        {:via, Registry, {SpiderMan.Registry, {spider, component, base_name}}}
      end

      @impl true
      def handle_failed(messages, %{max_retries: 0}), do: messages
      def handle_failed(messages, %{max_retries: :infinity}), do: messages

      def handle_failed(messages, context) do
        Enum.map(messages, fn message ->
          Broadway.Message.update_data(message, fn event ->
            Map.update!(event, :retries, &(&1 - 1))
          end)
        end)
      end
    end
  end

  def push_to_next_component(%{next_tid: next_tid, tid: tid}, events) do
    events
    |> Enum.flat_map(fn
      list when is_list(list) ->
        Enum.map(list, &{&1.key, %{&1 | options: [{:prev_tid, tid} | &1.options]}})

      data ->
        [{data.key, %{data | options: [{:prev_tid, tid} | data.options]}}]
    end)
    |> case do
      [] -> :skip
      events -> :ets.insert(next_tid, events)
    end
  end
end
