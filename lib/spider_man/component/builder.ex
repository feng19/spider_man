defmodule SpiderMan.Component.Builder do
  @moduledoc false
  require Logger
  alias Broadway.{Message, Acknowledger}
  alias SpiderMan.Pipeline
  @behaviour Acknowledger

  defmacro __using__(_opts \\ []) do
    quote do
      use Broadway

      def start_link(options) do
        unquote(__MODULE__).start_link(__MODULE__, options)
      end

      @impl true
      def handle_failed(messages, %{max_retries: 0}), do: messages
      def handle_failed(messages, %{max_retries: :infinity}), do: messages

      def handle_failed(messages, _context) do
        Enum.map(messages, fn message ->
          Message.update_data(message, fn event ->
            Map.update!(event, :retries, &(&1 - 1))
          end)
        end)
      end
    end
  end

  def start_link(component, options) do
    options = transform_broadway_options(component, options)
    Broadway.start_link(component, options)
  end

  defp transform_broadway_options(component, options) do
    options = Map.new(options)
    spider = options.spider
    opts = Map.merge(%{next_tid: nil, pipelines: [], additional_specs: []}, options)
    {pipelines, opts} = Pipeline.prepare_for_start(opts.pipelines, opts)

    Logger.info(
      "!! spider: #{inspect(spider)}, component: #{inspect(component)} setup prepare_for_start_pipelines finish."
    )

    ets_producer_options = opts |> Map.take([:tid, :additional_specs]) |> Map.to_list()
    processor = Map.get(opts, :processor, [])

    producer = [
      module: {SpiderMan.Producer.ETS, ets_producer_options},
      transformer: {__MODULE__, :transform, [Map.take(opts, [:tid, :next_tid])]}
    ]

    producer =
      case Map.get(opts, :rate_limiting) do
        rate_limiting when is_list(rate_limiting) ->
          [{:rate_limiting, rate_limiting} | producer]

        _ ->
          producer
      end

    context =
      opts
      |> Map.get(:context, %{})
      |> Map.merge(%{spider: spider, pipelines: pipelines})

    [
      name: :"#{inspect(spider)}.#{Module.split(component) |> List.last()}",
      producer: producer,
      processors: [default: processor],
      batchers: Map.get(opts, :batchers, []),
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
  def ack(%{tid: tid, next_tid: next_tid}, successful, failed) when next_tid != nil do
    # push successful events to next_tid
    successful
    |> Enum.flat_map(fn
      %{data: list} when is_list(list) ->
        Enum.map(list, &{&1.key, %{&1 | options: [{:prev_tid, tid} | &1.options]}})

      %{data: data} ->
        [{data.key, %{data | options: [{:prev_tid, tid} | data.options]}}]
    end)
    |> case do
      [] -> :skip
      successful_events -> :ets.insert(next_tid, successful_events)
    end

    requeue_failed(tid, failed)
  end

  def ack(%{tid: tid}, _successful, failed), do: requeue_failed(tid, failed)

  defp requeue_failed(nil, _failed), do: :ok

  defp requeue_failed(tid, failed) do
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
