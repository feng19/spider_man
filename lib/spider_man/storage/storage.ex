defmodule SpiderMan.Storage do
  @moduledoc false

  @type storage_context :: map

  @callback store(batcher :: atom, items :: Enumerable.t(), storage_context) ::
              :ok | {:error, term} | [:ok] | [{:error, term}]
  @callback prepare_for_start(arg :: term, options) :: options | {storage_context, options}
            when options: keyword
  @callback prepare_for_stop(options :: keyword) :: :ok
  @optional_callbacks prepare_for_start: 2, prepare_for_stop: 1

  alias SpiderMan.Storage.Multi

  def prepare_for_start(options) do
    case Keyword.get(options, :storage) do
      false ->
        false

      _ ->
        case Keyword.get(options, :batchers, []) do
          [] ->
            Keyword.delete(options, :storage)

          _ ->
            options
            |> Keyword.get(:storage, SpiderMan.Storage.JsonLines)
            |> prepare_for_start(options)
        end
    end
  end

  defp prepare_for_start(storage, options) when is_atom(storage) do
    prepare_for_start({storage, nil}, options)
  end

  defp prepare_for_start({storage, arg}, options) when is_atom(storage) do
    spider = Keyword.fetch!(options, :spider)

    {storage_context, options} =
      with {:module, _} <- Code.ensure_loaded(storage),
           true <- function_exported?(storage, :prepare_for_start, 2) do
        storage.prepare_for_start(arg, options)
      else
        {:error, _} -> raise "Storage module: #{inspect(storage)} undefined."
        _ -> options
      end
      |> case do
        return = {storage_context, options} when is_map(storage_context) and is_list(options) ->
          return

        options when is_list(options) ->
          context = Keyword.get(options, :context, %{})
          storage_context = Map.get(context, :storage_context, %{})
          {storage_context, options}

        return ->
          raise "Wrong value: #{inspect(return)} return by Storage: #{inspect(storage)}."
      end

    storage_context = Map.put_new(storage_context, :spider, spider)

    context =
      Keyword.get(options, :context, %{})
      |> Map.merge(%{storage: storage, storage_context: storage_context})

    Keyword.merge(options, storage: storage, context: context)
  end

  defp prepare_for_start(storage_list, options) when is_list(storage_list) do
    {storage_list, options} =
      storage_list
      |> Stream.filter(&(is_atom(&1) or match?({s, _} when is_atom(s), &1)))
      |> Enum.map_reduce(options, fn storage, options ->
        {context, options} =
          prepare_for_start(storage, options)
          |> Keyword.pop(:context)

        {storage, context} = Map.pop(context, :storage)
        {storage_context, context} = Map.pop(context, :storage_context)
        options = Keyword.delete(options, :storage) |> Keyword.put(:context, context)
        {%{storage: storage, storage_context: storage_context}, options}
      end)

    context =
      Keyword.get(options, :context)
      |> Map.merge(%{storage: Multi, storage_context: %{storage_list: storage_list}})

    Keyword.merge(options, storage: Multi, context: context)
  end

  defp prepare_for_start(nil, options), do: options

  def prepare_for_stop(false), do: :ok

  def prepare_for_stop(options) do
    case Keyword.get(options, :storage) do
      nil ->
        :skip

      Multi ->
        context = Keyword.get(options, :context)
        Enum.each(context.storage_context.storage_list, &call_storage_stop(&1, options, context))

      storage ->
        if function_exported?(storage, :prepare_for_stop, 1) do
          storage.prepare_for_stop(options)
        end
    end
  end

  defp call_storage_stop(%{storage: storage, storage_context: storage_context}, options, context) do
    if function_exported?(storage, :prepare_for_stop, 1) do
      context = Map.merge(context, %{storage: storage, storage_context: storage_context})
      options = Keyword.merge(options, storage: storage, context: context)
      storage.prepare_for_stop(options)
    end
  end
end
