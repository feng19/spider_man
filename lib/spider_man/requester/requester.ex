defmodule SpiderMan.Requester do
  @moduledoc false

  @callback request(url :: binary, options :: [Tesla.option()], context :: map) :: Tesla.result()
  @callback prepare_for_start(arg :: term, options) :: options when options: keyword
  @callback prepare_for_stop(options :: keyword) :: :ok
  @optional_callbacks prepare_for_start: 2, prepare_for_stop: 1

  def prepare_for_start(options) do
    options
    |> Keyword.get(:requester, {SpiderMan.Requester.Finch, []})
    |> prepare_for_start(options)
  end

  defp prepare_for_start(requester, options) when is_atom(requester) do
    prepare_for_start({requester, nil}, options)
  end

  defp prepare_for_start({requester, arg}, options) when is_atom(requester) do
    options =
      Keyword.update(
        options,
        :context,
        %{requester: requester},
        &Map.put(&1, :requester, requester)
      )

    with {:module, _} <- Code.ensure_loaded(requester),
         true <- function_exported?(requester, :prepare_for_start, 2) do
      requester.prepare_for_start(arg, options)
    else
      {:error, _} -> raise "Requester module: #{inspect(requester)} undefined."
      false -> options
    end
    |> Keyword.put(:requester, requester)
  end

  def prepare_for_stop(options) do
    requester = Keyword.fetch!(options, :requester)

    if function_exported?(requester, :prepare_for_stop, 1) do
      requester.prepare_for_stop(options)
    end
  end

  def append_default_middlewares(false, requester_options), do: requester_options[:middlewares]

  def append_default_middlewares(true, requester_options) do
    middlewares =
      if base_url = requester_options[:base_url] do
        [{Tesla.Middleware.BaseUrl, base_url} | requester_options[:middlewares]]
      else
        requester_options[:middlewares]
      end

    middlewares =
      if requester_options[:logging?] do
        middlewares ++ [Tesla.Middleware.Logger]
      else
        middlewares
      end

    if not_found_middleware?(middlewares, Tesla.Middleware.Retry) do
      retry_options = [
        delay: 500,
        max_retries: 3,
        max_delay: 4_000,
        should_retry: fn
          {:ok, %{status: status}} when status in [400, 500] -> true
          {:ok, _} -> false
          {:error, _} -> true
        end
      ]

      [{Tesla.Middleware.Retry, retry_options} | middlewares]
    else
      middlewares
    end
  end

  defp not_found_middleware?(middlewares, middleware) do
    Enum.all?(middlewares, fn
      {^middleware, _} -> false
      ^middleware -> false
      _ -> true
    end)
  end
end
