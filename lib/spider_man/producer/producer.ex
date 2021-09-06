defmodule SpiderMan.Producer do
  @moduledoc false

  @type producer :: {module, keyword}
  @callback producer_settings(arg :: term, options) :: {producer, options} when options: keyword
  @optional_callbacks producer_settings: 2

  def process_name(spider, component),
    do: {:via, Registry, {SpiderMan.Registry, {spider, component}}}

  def producer_status(spider, component), do: call_producer(spider, component, :status)

  def call_producer(spider, component, msg) do
    [producer_name] = process_name(spider, component) |> Broadway.producer_names()
    GenStage.call(producer_name, msg)
  end

  def prepare_for_start_producer(false), do: false

  def prepare_for_start_producer(options),
    do: prepare_for_start_producer(options[:producer], options)

  defp prepare_for_start_producer(producer, options) when is_atom(producer) do
    prepare_for_start_producer({producer, nil}, options)
  end

  defp prepare_for_start_producer({producer, arg}, options) when is_atom(producer) do
    {producer, options} =
      with {:module, _} <- Code.ensure_loaded(producer),
           true <- function_exported?(producer, :producer_settings, 2) do
        producer.producer_settings(arg, options)
      else
        {:error, _} -> raise "Producer module: #{inspect(producer)} undefined."
        _ -> options
      end

    producer =
      case Keyword.get(options, :rate_limiting) do
        rate_limiting when is_list(rate_limiting) ->
          [{:rate_limiting, rate_limiting} | producer]

        _ ->
          producer
      end

    Keyword.put(options, :producer, producer)
  end
end
