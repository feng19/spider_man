defmodule SpiderMan.Storage.JsonLines do
  @moduledoc false
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

    file_path |> Path.dirname() |> File.mkdir_p!()

    case File.open(file_path, [:write, :append, :binary]) do
      {:ok, io_device} ->
        storage_context =
          Keyword.get(options, :context, %{})
          |> Map.get(:storage_context, %{})
          |> Map.merge(%{io_device: io_device, file_path: file_path})

        {storage_context, options}

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
