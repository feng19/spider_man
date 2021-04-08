if Code.ensure_loaded?(Telemetry.Metrics) do
  defmodule SpiderMan.Telemetry do
    @moduledoc false
    import Telemetry.Metrics

    def tag_values_fun(count) do
      &Map.update!(&1, :name, fn name ->
        name |> Module.split() |> Enum.slice(0..count)
      end)
    end

    def metrics(tag_values \\ tag_values_fun(2)) do
      [
        # spider_man
        summary("spider_man.ets.memory", unit: {:byte, :kilobyte}, tags: [:name, :component]),
        summary("spider_man.ets.size", tags: [:name, :component]),
        # broadway
        counter("broadway.processor.message.start.time", tags: [:name], tag_values: tag_values),
        counter("broadway.processor.message.stop.time", tags: [:name], tag_values: tag_values),
        summary("broadway.processor.message.stop.duration",
          tags: [:name],
          tag_values: tag_values,
          unit: {:native, :millisecond}
        ),
        counter("broadway.consumer.start.time", tags: [:name], tag_values: tag_values),
        counter("broadway.consumer.stop.time", tags: [:name], tag_values: tag_values),
        summary("broadway.consumer.stop.duration",
          tags: [:name],
          tag_values: tag_values,
          unit: {:native, :millisecond}
        )
      ]
    end
  end
end
