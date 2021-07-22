defmodule SpiderMan.Storage.CSV do
  @moduledoc """
  Save items to *.csv files by Storage

  ## Usage

      settings = [
        ...
        item_processor_options: [
          storage: [{#{inspect(__MODULE__)}, [headers: headers, file: file_name]}]
        ]
      ]

  If didn't set `:file` for this Storage, the default is `./data/Spider_Second.csv`.
  """
  @behaviour SpiderMan.Storage
  alias SpiderMan.Utils

  @impl true
  def store(_, items, %{io_device: io_device, header_keys: header_keys}) do
    iodata =
      Stream.map(items, & &1.value)
      |> Stream.map(&take(&1, header_keys))
      |> NimbleCSV.RFC4180.dump_to_iodata()

    IO.write(io_device, iodata)
  end

  defp take(item, header_keys) do
    Enum.map(header_keys, &Map.get(item, &1))
  end

  @impl true
  def prepare_for_start([headers: headers], options) when is_list(headers) do
    file_path =
      Keyword.fetch!(options, :spider)
      |> Utils.get_file_path_by_spider("csv")

    prepare_for_start([file: file_path, headers: headers], options)
  end

  def prepare_for_start(arg, options) when is_list(arg) do
    unless Code.ensure_loaded?(NimbleCSV.RFC4180) do
      raise "Please add :nimble_csv lib to your deps."
    end

    %{file: file_path, headers: headers} = Map.new(arg)

    unless is_binary(file_path) and is_list(headers) do
      raise "Wrong type of file: #{inspect(file_path)} or headers: #{inspect(headers)} when using #{inspect(__MODULE__)}."
    end

    file_path |> Path.dirname() |> File.mkdir_p!()

    {header_keys, headers} =
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

    case File.open(file_path, [:write, :append, :binary, :utf8]) do
      {:ok, io_device} ->
        # headers line
        iodata = NimbleCSV.RFC4180.dump_to_iodata([headers])
        :ok = IO.write(io_device, iodata)

        storage_context =
          Keyword.get(options, :context, %{})
          |> Map.get(:storage_context, %{})
          |> Map.merge(%{
            io_device: io_device,
            file_path: file_path,
            header_keys: header_keys
          })

        {storage_context, options}

      error ->
        raise "Can't open file: #{file_path} with error: #{inspect(error)}."
    end
  end

  @impl true
  def prepare_for_stop(options) do
    context = options[:context]
    io_device = context.storage_context.io_device
    :ok = File.close(io_device)
    :ok
  end
end
