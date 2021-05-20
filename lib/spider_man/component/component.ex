defmodule SpiderMan.Component do
  @moduledoc false

  defmacro __using__(_opts \\ []) do
    quote location: :keep do
      use Broadway
      require Logger

      def start_link(options) do
        processor = Keyword.get(options, :processor, [])

        Broadway.start_link(__MODULE__,
          name: SpiderMan.Producer.process_name(options[:spider], __MODULE__),
          producer: options[:producer],
          processors: [default: processor],
          batchers: Keyword.get(options, :batchers, []),
          context: options[:context]
        )
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
end
