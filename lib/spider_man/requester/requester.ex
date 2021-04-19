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
end
