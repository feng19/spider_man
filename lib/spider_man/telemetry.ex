if Code.ensure_loaded?(Telemetry.Metrics) do
  defmodule SpiderMan.Telemetry do
    @moduledoc false
    import Telemetry.Metrics

    def tag_values_fun(count) do
      &Map.update!(&1, :name, fn {:via, _, {_, tuple}} ->
        tuple |> Tuple.to_list() |> Enum.slice(0..count) |> Enum.join("-")
      end)
    end

    def metrics(tag_values \\ tag_values_fun(1)) do
      reporter_options = [nav: "spider_man"]
      metric_options = [tags: [:name], tag_values: tag_values, reporter_options: reporter_options]

      [
        # spider_man
        summary("spider_man.ets.memory", unit: {:byte, :kilobyte}, tags: [:name, :tid]),
        summary("spider_man.ets.size", tags: [:name, :tid]),
        # broadway
        counter("broadway.processor.message.start.time", metric_options),
        counter("broadway.processor.message.stop.time", metric_options),
        summary("broadway.processor.message.stop.duration", [
          {:unit, {:native, :millisecond}} | metric_options
        ]),
        counter("broadway.batch_processor.start.time", metric_options),
        counter("broadway.batch_processor.stop.time", metric_options),
        summary("broadway.batch_processor.stop.duration", [
          {:unit, {:native, :millisecond}} | metric_options
        ])
      ]
    end
  end
end
