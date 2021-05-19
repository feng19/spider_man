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

    options =
      Map.merge(
        %{next_tid: nil, pipelines: [], post_pipelines: [], additional_specs: []},
        options
      )

    {pipelines, options} = Pipeline.prepare_for_start(options.pipelines, options)
    {post_pipelines, options} = Pipeline.prepare_for_start(options.post_pipelines, options)

    Logger.info(
      "!! spider: #{inspect(spider)}, component: #{inspect(component)} pipelines setup prepare_for_start finish."
    )

    default_producer_options = options |> Map.take([:tid, :status]) |> Map.to_list()

    producer_module =
      case options.producer do
        producer when is_atom(producer) ->
          {producer, default_producer_options}

        {producer, producer_options} when is_atom(producer) and is_list(producer_options) ->
          {producer, producer_options ++ default_producer_options}
      end

    producer_settings = [
      module: producer_module,
      transformer: {__MODULE__, :transform, [Map.take(options, [:tid, :failed_tid, :component])]}
    ]

    producer_settings =
      case Map.get(options, :rate_limiting) do
        rate_limiting when is_list(rate_limiting) ->
          [{:rate_limiting, rate_limiting} | producer_settings]

        _ ->
          producer_settings
      end

    processor = Map.get(options, :processor, [])

    context =
      options
      |> Map.get(:context, %{})
      |> Map.merge(%{pipelines: pipelines, post_pipelines: post_pipelines})
      |> Map.merge(Map.take(options, [:tid, :next_tid, :spider, :spider_module]))

    [
      name: process_name(spider, component),
      producer: producer_settings,
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
  def ack(%{tid: tid, failed_tid: failed_tid, component: component}, _successful, failed) do
    {events, failed_events} =
      failed
      |> Stream.reject(&match?({:failed, :skiped}, &1.status))
      |> Stream.map(& &1.data)
      |> Enum.split_with(&(&1.retries > 0))

    failed_objects = Enum.map(failed_events, &{{component, &1.key}, &1})
    :ets.insert(failed_tid, failed_objects)

    retry_events(events, tid)
  end

  defp retry_events([], _tid), do: :ok

  defp retry_events(events, tid) do
    objects = Enum.map(events, &{&1.key, &1})
    :ets.insert(tid, objects)
  end
end
