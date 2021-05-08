defmodule SpiderMan.Component.Builder do
  @moduledoc false
  require Logger
  alias Broadway.{Message, Acknowledger}
  alias SpiderMan.Pipeline
  @behaviour Acknowledger

  defmacro __using__(_opts \\ []) do
    quote do
      use Broadway
      require Logger

      def start_link(options) do
        unquote(__MODULE__).start_link(__MODULE__, options)
      end

      @impl true
      def handle_failed(messages, %{max_retries: 0}), do: messages
      def handle_failed(messages, %{max_retries: :infinity}), do: messages

      def handle_failed(messages, context) do
        if context[:debug] do
          Logger.debug("handle_failed messages: #{inspect(messages)}")
        end

        Enum.map(messages, fn message ->
          Message.update_data(message, fn event ->
            Map.update!(event, :retries, &(&1 - 1))
          end)
        end)
      end
    end
  end

  def process_name(spider, component), do: :"#{spider}.#{Module.split(component) |> List.last()}"

  def start_link(component, options) do
    options = transform_broadway_options(component, options)
    Broadway.start_link(component, options)
  end

  defp transform_broadway_options(component, options) do
    options = Map.new(options)
    spider = options.spider
    options = Map.merge(%{next_tid: nil, pipelines: [], additional_specs: []}, options)
    {pipelines, options} = Pipeline.prepare_for_start(options.pipelines, options)
    options = Map.put(options, :pipelines, pipelines)

    Logger.info(
      "!! spider: #{inspect(spider)}, component: #{inspect(component)} pipelines setup prepare_for_start finish."
    )

    ets_producer_options =
      options
      |> Map.take([:tid, :additional_specs, :status, :buffer_size, :buffer_keep, :retry_interval])
      |> Map.to_list()

    processor = Map.get(options, :processor, [])

    producer = [
      module: {SpiderMan.Producer.ETS, ets_producer_options},
      transformer: {__MODULE__, :transform, [Map.take(options, [:tid, :next_tid])]}
    ]

    producer =
      case Map.get(options, :rate_limiting) do
        rate_limiting when is_list(rate_limiting) ->
          [{:rate_limiting, rate_limiting} | producer]

        _ ->
          producer
      end

    context =
      options
      |> Map.get(:context, %{})
      |> Map.merge(Map.take(options, [:tid, :next_tid, :spider, :spider_module, :pipelines]))

    [
      name: process_name(spider, component),
      producer: producer,
      processors: [default: processor],
      batchers: Map.get(options, :batchers, []),
      context: context
    ]
  end

  def transform({_key, event}, [info]) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, info, nil}
    }
  end

  @impl Acknowledger
  def ack(%{tid: nil}, _successful, _failed), do: :ok

  def ack(%{tid: tid}, _successful, failed) do
    failed
    |> Stream.reject(&match?({:failed, :skiped}, &1.status))
    |> Stream.map(& &1.data)
    |> Enum.reject(&(&1.retries <= 0))
    |> case do
      [] ->
        :ok

      events ->
        objects = Enum.map(events, &{&1.key, &1})
        :ets.insert(tid, objects)
    end
  end
end
