defmodule SpiderMan.Producer.ETS do
  @moduledoc false
  use GenStage
  require Logger
  alias Broadway.{Producer, Message, Acknowledger}

  @behaviour SpiderMan.Producer
  @behaviour Producer
  @behaviour Acknowledger

  @impl SpiderMan.Producer
  def producer_settings(producer_options, options) do
    default_producer_options = Keyword.take(options, [:tid, :status])

    producer_options =
      if is_list(producer_options) do
        producer_options ++ default_producer_options
      else
        default_producer_options
      end

    ack_info = Keyword.take(options, [:tid, :failed_tid, :component]) |> Map.new()

    producer_settings = [
      module: {__MODULE__, producer_options},
      transformer: {__MODULE__, :transform, [ack_info]}
    ]

    context =
      Map.merge(
        options[:context],
        Keyword.take(options, [:tid, :next_tid, :spider, :spider_module]) |> Map.new()
      )

    {producer_settings, Keyword.put(options, :context, context)}
  end

  @impl GenStage
  def init(opts) do
    {gen_stage_opts, opts} = Keyword.split(opts, [:buffer_size, :buffer_keep])
    tid = Keyword.fetch!(opts, :tid)
    retry_interval = Keyword.get(opts, :retry_interval, 200)
    status = Keyword.get(opts, :status, :running)

    retry_timer =
      case status do
        :running -> nil
        _ -> false
      end

    state = %{
      name: get_in(opts, [:broadway, :name]),
      status: status,
      tid: tid,
      retry_interval: retry_interval,
      retry_timer: retry_timer,
      demand: 0
    }

    {:producer, state, gen_stage_opts}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    handle_get_messages(%{state | demand: state.demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:retry_get_messages, state) do
    handle_get_messages(%{state | retry_timer: nil})
  end

  @impl GenStage
  def handle_call(:status, _from, state), do: {:reply, state.status, [], state}

  def handle_call(:suspend, _from, %{status: :running, retry_timer: retry_timer} = state) do
    retry_timer && Process.cancel_timer(retry_timer)
    {:reply, :ok, [], %{state | retry_timer: false, status: :suspended}}
  end

  def handle_call(:suspend, _from, state), do: {:reply, :ok, [], state}

  def handle_call(:continue, from, %{status: :suspended} = state) do
    GenStage.reply(from, :ok)
    handle_get_messages(%{state | retry_timer: nil, status: :running})
  end

  def handle_call(:continue, _from, state), do: {:reply, :ok, [], state}

  defp handle_get_messages(%{retry_timer: nil, demand: demand, tid: tid} = state)
       when demand > 0 do
    messages =
      case :ets.match_object(tid, :_, demand) do
        {messages, _} ->
          Enum.map(messages, fn {_key, data} = object ->
            :ets.delete_object(tid, object)
            data
          end)

        :"$end_of_table" ->
          []
      end

    new_demand = demand - length(messages)
    retry_interval = state.retry_interval

    retry_timer =
      case new_demand do
        0 -> nil
        ^demand -> start_timer(retry_interval)
        _ -> start_timer(retry_interval)
      end

    {:noreply, messages, %{state | demand: new_demand, retry_timer: retry_timer}}
  end

  defp handle_get_messages(state) do
    {:noreply, [], state}
  end

  @impl Producer
  def prepare_for_start(_module, options) do
    {_, producer_options} = options[:producer][:module]
    specs = Keyword.get(producer_options, :additional_specs, [])
    {specs, options}
  end

  @impl Producer
  def prepare_for_draining(%{retry_timer: retry_timer} = state) do
    retry_timer && Process.cancel_timer(retry_timer)
    {:noreply, [], %{state | retry_timer: nil}}
  end

  @compile {:inline, start_timer: 1}
  defp start_timer(interval) do
    Process.send_after(self(), :retry_get_messages, interval)
  end

  def transform(data, [ack_info]) do
    %Message{
      data: data,
      acknowledger: {__MODULE__, ack_info, nil}
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
