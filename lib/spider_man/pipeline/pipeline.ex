defmodule SpiderMan.Pipeline do
  @moduledoc false
  require Logger

  @type event :: any
  @type arg :: any
  @type call_return :: {:ok, event} | {:error, error :: any} | :skiped | event
  @type t :: module | {module, function_name :: atom()} | mfa | (term -> call_return)

  @callback call(event, arg) :: call_return
  @callback prepare_for_start(arg, options) :: {arg, options} when options: keyword
  @callback prepare_for_stop(arg) :: :ok
  @optional_callbacks call: 2, prepare_for_start: 2, prepare_for_stop: 1

  def call(pipelines, acc) do
    Enum.reduce_while(pipelines, acc, &do_call(&1, &2))
  end

  defp do_call(pipeline, acc) do
    case pipeline do
      {m, f, arg} -> apply(m, f, [acc, arg])
      {fun, arg} -> fun.(arg, acc)
      fun -> fun.(acc)
    end
    |> case do
      {:ok, acc} -> {:cont, acc}
      {:error, _} = error -> {:halt, error}
      acc -> {:cont, acc}
    end
  rescue
    reason ->
      Logger.error(Exception.format(:error, reason, __STACKTRACE__))
      {:halt, {:error, reason}}
  catch
    error, reason ->
      Logger.error(Exception.format(error, reason, __STACKTRACE__))
      {:halt, {:error, reason}}
  end

  def prepare_for_start(pipelines, options) do
    Enum.map_reduce(pipelines, options, &do_prepare_for_start/2)
  end

  defp do_prepare_for_start(fun, options) when is_function(fun, 1), do: {fun, options}
  defp do_prepare_for_start(fun, options) when is_function(fun, 2), do: {{fun, nil}, options}

  defp do_prepare_for_start({fun, arg}, options) when is_function(fun, 2),
    do: {{fun, arg}, options}

  defp do_prepare_for_start(pipeline, options) do
    {m, f, arg} =
      case pipeline do
        {m, arg} when is_atom(m) -> {m, :call, arg}
        {m, f, _arg} when is_atom(m) and is_atom(f) -> pipeline
        m when is_atom(m) -> {m, :call, nil}
      end

    {arg, options} =
      with {:module, _} <- Code.ensure_loaded(m),
           true <- function_exported?(m, f, 2),
           {_, true} <- {:start, function_exported?(m, :prepare_for_start, 2)} do
        m.prepare_for_start(arg, options)
      else
        {:error, _} -> raise "Pipeline module: #{inspect(m)} undefined!"
        false -> raise "Pipeline module: #{inspect(m)} undefined function #{to_string(f)}/2!"
        {:start, false} -> {arg, options}
      end

    {{m, f, arg}, options}
  end

  def prepare_for_stop(pipelines) do
    Enum.each(pipelines, &do_prepare_for_stop/1)
  end

  defp do_prepare_for_stop({m, _f, arg}) do
    if function_exported?(m, :prepare_for_stop, 1) do
      m.prepare_for_stop(arg)
    end
  end

  defp do_prepare_for_stop(_), do: :ok
end
