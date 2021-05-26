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
  def prepare_for_start(file_path, options) when is_binary(file_path) do
    file_path |> Path.dirname() |> File.mkdir_p!()

    tid = :ets.new(__MODULE__, [:set, :public, write_concurrency: true, read_concurrency: true])

    storage_context =
      Keyword.get(options, :context, %{})
      |> Map.get(:storage_context, %{})
      |> Map.merge(%{file_path: file_path, tid: tid})

    {storage_context, options}
  end

  def prepare_for_start(_, options) do
    Keyword.fetch!(options, :spider)
    |> Utils.get_file_path_by_spider("ets")
    |> prepare_for_start(options)
  end

  @impl true
  def prepare_for_stop(options) do
    context = options[:context]
    %{file_path: file_path, tid: tid} = context.storage_context
    Logger.notice("starting dump ets to file: #{file_path} ...")
    file_name = String.to_charlist(file_path)
    result = :ets.tab2file(tid, file_name, extended_info: [:md5sum], sync: true)
    Logger.notice("dump ets to file: #{file_name} finished, result: #{inspect(result)}.")
    :ets.delete(tid)
    :ok
  end
end
