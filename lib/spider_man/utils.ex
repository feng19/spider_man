defmodule SpiderMan.Utils do
  @moduledoc false
  require Logger
  alias SpiderMan.{Request, Item}

  def build_request(url, options \\ [method: :get], max_retries \\ 3) do
    %Request{key: url, url: url, options: options, retries: max_retries}
  end

  def build_item(key, value, options \\ [], max_retries \\ 0) do
    %Item{key: key, value: value, options: options, retries: max_retries}
  end

  def pipe(pipeline, acc) do
    case pipeline do
      {m, f, arg} -> apply(m, f, [acc, arg])
      fun -> fun.(acc)
    end
    |> case do
      {:ok, acc} -> {:cont, acc}
      {:error, _} = error -> {:halt, error}
      acc -> {:cont, acc}
    end
  rescue
    e ->
      Logger.error(Exception.message(e))
      {:halt, {:error, e}}
  catch
    error, reason ->
      Logger.error(Exception.format(error, reason, __STACKTRACE__))
      {:halt, {:error, reason}}
  end

  def merge_settings(old_settings, new_settings) do
    Keyword.merge(old_settings, new_settings, fn _k, v1, v2 ->
      Keyword.merge(v1, v2, fn
        :middlewares, _m1, m2 ->
          m2

        :batchers, _b1, b2 ->
          b2

        _sk, sv1, sv2 when is_list(sv1) and is_list(sv2) ->
          Keyword.merge(sv1, sv2)

        _sk, sv1, sv2 when is_map(sv1) and is_map(sv2) ->
          Map.merge(sv1, sv2)

        _sk, _sv1, sv2 ->
          sv2
      end)
    end)
  end

  def call_middleware_prepare_for_start(fun, _options) when is_function(fun, 1), do: fun

  def call_middleware_prepare_for_start(middleware, options) do
    {m, f, arg} =
      case middleware do
        {m, arg} when is_atom(m) -> {m, :call, arg}
        {m, f, args} when is_atom(m) and is_atom(f) and is_list(args) -> middleware
        m when is_atom(m) -> {m, :call, []}
      end

    arg =
      if function_exported?(m, :prepare_for_start, 2) do
        m.prepare_for_start(arg, options)
      else
        arg
      end

    {m, f, arg}
  end

  def call_middleware_prepare_for_stop({m, _f, arg}) do
    if function_exported?(m, :prepare_for_stop, 2) do
      m.prepare_for_stop(arg)
    end
  end

  def call_middleware_prepare_for_stop(_), do: :ok

  def call_producer(broadway, msg) do
    [producer_name] = Broadway.producer_names(broadway)
    GenStage.call(producer_name, msg)
  end
end
