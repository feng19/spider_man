if Code.ensure_loaded?(Telemetry.Metrics) do
  defmodule SpiderMan.Telemetry do
    @moduledoc false
    import Telemetry.Metrics

    def tag_values_fun(count) do
      &Map.update!(&1, :name, fn name ->
        name |> Module.split() |> Enum.slice(0..count) |> Enum.join(".")
      end)
    end

    def metrics(tag_values \\ tag_values_fun(2)) do
      reporter_options = [nav: "spider_man"]
      metric_options = [tags: [:name], tag_values: tag_values, reporter_options: reporter_options]

      [
        # spider_man
        summary("spider_man.ets.memory", unit: {:byte, :kilobyte}, tags: [:name, :component]),
        summary("spider_man.ets.size", tags: [:name, :component]),
        # broadway
        counter("broadway.processor.message.start.time", metric_options),
        counter("broadway.processor.message.stop.time", metric_options),
        summary("broadway.processor.message.stop.duration", [
          {:unit, {:native, :millisecond}} | metric_options
        ]),
        counter("broadway.consumer.start.time", metric_options),
        counter("broadway.consumer.stop.time", metric_options),
        summary("broadway.consumer.stop.duration", [
          {:unit, {:native, :millisecond}} | metric_options
        ])
      ]
    end
  end
end
