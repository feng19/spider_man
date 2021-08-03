defmodule SpiderMan.Stats do
  @moduledoc false
  @events [
    [:spider_man, :downloader, :start],
    [:spider_man, :downloader, :stop],
    [:spider_man, :spider, :start],
    [:spider_man, :spider, :stop],
    [:spider_man, :item_processor, :start],
    [:spider_man, :item_processor, :stop]
  ]

  def attach_spider_stats(spider, tid) do
    name = inspect(spider)

    for event <- @events do
      id = {__MODULE__, event, self()}
      :telemetry.attach(id, event, &__MODULE__.update_spider_stats/4, {name, tid})
    end
  end

  def detach_spider_stats do
    for event <- @events do
      :telemetry.detach({__MODULE__, event, self()})
    end
  end

  if Mix.env() != :test do
    def print_spider_stats(tid), do: IO.write("\e[2K\r#{format_stats(tid)} ")
  else
    def print_spider_stats(tid), do: format_stats(tid)
  end

  defp format_stats(tid) do
    [downloader, item_processor, spider] = :ets.tab2list(tid) |> Enum.sort()

    [downloader, spider, item_processor]
    |> Enum.map(&format_component_stats/1)
    |> Enum.join(" ")
  end

  defp format_component_stats({component, total, success, fail, duration}) do
    tps =
      case System.convert_time_unit(duration, :native, :millisecond) do
        0 ->
          0

        ms ->
          tps = Float.floor(success / (ms / 1000), 2)

          if tps > 999 do
            "999+"
          else
            tps
          end
      end

    component = Atom.to_string(component) |> Macro.camelize()
    "#{component}:[#{success}/#{total} #{tps}/s F:#{fail}]"
  end

  def update_spider_stats([_, component, :start], measurements, metadata, {name, tid}) do
    if match?(%{name: ^name}, metadata) do
      :ets.update_counter(tid, component, {2, measurements.count})
    end
  end

  def update_spider_stats([_, component, :stop], measurements, metadata, {name, tid}) do
    if match?(%{name: ^name}, metadata) do
      %{success: success, fail: fail, duration: duration} = measurements
      :ets.update_counter(tid, component, [{3, success}, {4, fail}, {5, duration}])
    end
  end
end

defmodule SpiderMan.Stats.Task do
  @moduledoc false
  use GenServer

  def start_link(state), do: GenServer.start_link(__MODULE__, state)
  def suspend(nil), do: :skiped
  def suspend(pid), do: GenServer.call(pid, :suspend)
  def continue(nil), do: :skiped
  def continue(pid), do: GenServer.call(pid, :continue)

  def init(state) do
    {:ok, state, 1000}
  end

  def handle_call(:suspend, _from, state) do
    {:reply, :ok, %{state | status: :suspended}}
  end

  def handle_call(:continue, _from, %{status: :suspended} = state) do
    Process.send_after(self(), :refresh, state.refresh_interval)
    {:reply, :ok, %{state | status: :running}}
  end

  def handle_call(:continue, _from, state) do
    {:reply, :ok, state}
  end

  def handle_info(:refresh, %{status: :running, refresh_interval: interval, tid: tid} = state) do
    Process.send_after(self(), :refresh, interval)
    SpiderMan.Stats.print_spider_stats(tid)
    {:noreply, state}
  end

  def handle_info(:refresh, state) do
    {:noreply, state}
  end

  def handle_info(:timeout, state) do
    Process.send_after(self(), :refresh, state.refresh_interval)
    {:noreply, state}
  end
end
