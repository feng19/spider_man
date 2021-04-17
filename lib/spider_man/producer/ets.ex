defmodule SpiderMan.Producer.ETS do
  @moduledoc false
  use GenStage
  require Logger
  alias Broadway.Producer

  @behaviour Producer

  @impl true
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

  @impl true
  def handle_demand(incoming_demand, state) do
    handle_get_messages(%{state | demand: state.demand + incoming_demand})
  end

  @impl true
  def handle_info(:retry_get_messages, state) do
    handle_get_messages(%{state | retry_timer: nil})
  end

  @impl true
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
          Enum.each(messages, &:ets.delete_object(tid, &1))
          messages

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
end
