defmodule SpiderMan.Storage.JsonLines do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Storage

  @impl true
  def store(_, items, %{io_device: io_device}) do
    Enum.map(
      items,
      fn item ->
        with {:ok, str} <- Jason.encode(item.value) do
          IO.write(io_device, [str, "\n"])
        end
      end
    )
  end

  @impl true
  def prepare_for_start(file_path, options) when is_binary(file_path) do
    unless Code.ensure_loaded?(Jason) do
      raise "Please add Jason lib to your deps."
    end

    dir = Path.dirname(file_path)
    File.mkdir_p!(dir)

    case File.open(file_path, [:write, :append, :binary]) do
      {:ok, io_device} ->
        storage_context = %{io_device: io_device, file_path: file_path}

        context =
          Keyword.get(options, :context, %{})
          |> Map.update(:storage_context, storage_context, &Map.merge(&1, storage_context))

        Keyword.put(options, :context, context)

      error ->
        raise "Can't open file: #{file_path} with error: #{inspect(error)}."
    end
  end

  def prepare_for_start(_, options) do
    spider = Keyword.fetch!(options, :spider)
    prepare_for_start("data/#{inspect(spider)}_#{System.system_time(:second)}.jsonl", options)
  end

  @impl true
  def prepare_for_stop(options) do
    context = options[:context]
    io_device = context.storage_context.io_device
    :ok = File.close(io_device)
    :ok
  end
end
