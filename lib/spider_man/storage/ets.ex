defmodule SpiderMan.Storage.ETS do
  @moduledoc false
  require Logger
  @behaviour SpiderMan.Storage
  alias SpiderMan.Utils

  @impl true
  def store(_batcher, items, %{tid: tid}) do
    objects = Enum.map(items, & &1.value)
    true = :ets.insert(tid, objects)
    :ok
  catch
    error, reason ->
      Logger.error(Exception.format(error, reason, __STACKTRACE__))
      {:error, "save2ets error"}
  end

  @impl true
  def prepare_for_start(nil, options) do
    Keyword.fetch!(options, :spider)
    |> Utils.get_file_path_by_spider("ets")
    |> prepare_for_start(options)
  end

  def prepare_for_start(file_path, options) when is_binary(file_path) do
    file_path |> Path.dirname() |> File.mkdir_p!()

    tid = :ets.new(__MODULE__, [:set, :public, write_concurrency: true, read_concurrency: true])

    storage_context =
      Keyword.get(options, :context, %{})
      |> Map.get(:storage_context, %{})
      |> Map.merge(%{file_path: file_path, tid: tid})

    {storage_context, options}
  end

  @impl true
  def prepare_for_stop(options) do
    context = options[:context]
    io_device = context.storage_context.io_device
    :ok = File.close(io_device)
    :ok
  end
end
