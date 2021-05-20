defmodule SpiderMan.Storage.CSV do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Storage

  @impl true
  def store(_, items, %{io_device: io_device, keys: keys}) do
    iodata =
      Stream.map(items, & &1.value)
      |> make_rows(keys)
      |> NimbleCSV.RFC4180.dump_to_iodata()

    IO.write(io_device, iodata)
  end

  @impl true
  def prepare_for_start(headers, options) when is_list(headers) do
    file_path = get_file_path_by_spider(options)
    prepare_for_start({file_path, headers}, options)
  end

  def prepare_for_start({file_path, headers}, options)
      when is_binary(file_path) and is_list(headers) do
    unless Code.ensure_loaded?(NimbleCSV.RFC4180) do
      raise "Please add NimbleCSV lib to your deps."
    end

    dir = Path.dirname(file_path)
    File.mkdir_p!(dir)

    {keys, headers} =
      Stream.map(headers, fn
        header when is_binary(header) ->
          {header, header}

        atom when is_atom(atom) ->
          {atom, Atom.to_string(atom)}

        {key, header} when is_binary(header) ->
          {key, header}

        {key, header} ->
          raise "Wrong type of #{inspect(header)} for key: #{inspect(key)}, Please use string for the header value."
      end)
      |> Enum.unzip()

    case File.open(file_path, [:write, :append, :binary]) do
      {:ok, io_device} ->
        # headers line
        iodata = NimbleCSV.RFC4180.dump_to_iodata([headers])
        :ok = IO.write(io_device, iodata)
        storage_context = %{io_device: io_device, file_path: file_path, keys: keys}

        context =
          Keyword.get(options, :context, %{})
          |> Map.update(:storage_context, storage_context, &Map.merge(&1, storage_context))

        Keyword.put(options, :context, context)

      error ->
        raise "Can't open file: #{file_path} with error: #{inspect(error)}."
    end
  end

  defp get_file_path_by_spider(options) do
    spider = Keyword.fetch!(options, :spider)
    "data/#{inspect(spider)}_#{System.system_time(:second)}.csv"
  end

  @impl true
  def prepare_for_stop(options) do
    context = options[:context]
    io_device = context.storage_context.io_device
    :ok = File.close(io_device)
    :ok
  end

  defp make_rows(list, header_keys) do
    header_keys = Enum.reverse(header_keys)
    Enum.map(list, &take(&1, header_keys))
  end

  defp take(item, keys) do
    Enum.reduce(keys, [], &Map.get(item, &1))
  end
end
