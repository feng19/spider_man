defmodule SpiderMan.Storage do
  @moduledoc false

  @type storage_context :: map

  @callback store(batcher :: atom, items :: Enumerable.t(), storage_context) ::
              [:ok] | [{:error, term}]
  @callback prepare_for_start(arg :: term, options) :: options when options: keyword
  @callback prepare_for_stop(options :: keyword) :: :ok
  @optional_callbacks prepare_for_start: 2, prepare_for_stop: 1

  def prepare_for_start(options) do
    options
    |> Keyword.get(:storage, SpiderMan.Storage.Log)
    |> prepare_for_start(options)
  end

  defp prepare_for_start(storage, options) when is_atom(storage) do
    prepare_for_start({storage, nil}, options)
  end

  defp prepare_for_start({storage, arg}, options) when is_atom(storage) do
    spider = Keyword.fetch!(options, :spider)

    options =
      with {:module, _} <- Code.ensure_loaded(storage),
           true <- function_exported?(storage, :prepare_for_start, 2) do
        storage.prepare_for_start(arg, options)
      else
        {:error, _} -> raise "Storage module: #{inspect(storage)} undefined."
        _ -> options
      end

    storage_context = Keyword.get(options, :storage_context, %{}) |> Map.put_new(:spider, spider)

    context =
      options
      |> Keyword.get(:context, %{})
      |> Map.merge(%{storage: storage, storage_context: storage_context})

    Keyword.merge(options, storage: storage, context: context)
  end

  defp prepare_for_start(nil, options), do: options

  def prepare_for_stop(options) do
    with storage when storage != nil <- Keyword.get(options, :storage),
         true <- function_exported?(storage, :prepare_for_stop, 1) do
      storage.prepare_for_stop(options)
    end
  end
end
