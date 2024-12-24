defmodule SpiderMan.Storage.JsonLines do
  @moduledoc """
  Save items to JsonLines(*.jsonl) file by Storage

  ## Usage

      settings = [
        ...
        item_processor_options: [
          storage: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, file_name}]
        ]
      ]

  If didn't set `file_name` for this Storage, the default is `./data/[Spider]_[Second].jsonl`,
  for example: `./data/spider_name_1707293252.jsonl`.
  """
  @behaviour SpiderMan.Storage
  @compile {:no_warn_undefined, Jason}
  alias SpiderMan.Utils

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
    if !Code.ensure_loaded?(Jason) do
      raise "Please add :jason lib to your deps."
    end

    file_path |> Path.dirname() |> File.mkdir_p!()

    case File.open(file_path, [:write, :append, :binary, :utf8]) do
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
    Keyword.fetch!(options, :spider)
    |> Utils.get_file_path_by_spider("jsonl")
    |> prepare_for_start(options)
  end

  @impl true
  def prepare_for_stop(options) do
    context = options[:context]
    io_device = context.storage_context.io_device
    :ok = File.close(io_device)
    :ok
  end
end
