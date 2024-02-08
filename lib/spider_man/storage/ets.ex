defmodule SpiderMan.Storage.ETS do
  @moduledoc """
  Save items to *.ets file by Storage

  ## Usage

      settings = [
        ...
        item_processor_options: [
          storage: [#{inspect(__MODULE__)} | {#{inspect(__MODULE__)}, file_name}]
        ]
      ]

  If didn't set `file_name` for this Storage, the default is `./data/[Spider]_[Second].ets`,
  for example: `./data/spider_name_1707293252.ets`.
  """
  require Logger
  @behaviour SpiderMan.Storage
  alias SpiderMan.Utils

  @impl true
  def store(_batcher, items, %{tid: tid}) do
    objects =
      Enum.map(items, fn
        %{value: value} when is_tuple(value) -> value
        %{key: key, value: value} -> {key, value}
      end)

    true = :ets.insert(tid, objects)
    :ok
  catch
    error, reason ->
      Logger.error(Exception.format(error, reason, __STACKTRACE__))
      {:error, "save2ets error"}
  end

  @impl true
  def prepare_for_start(%{file_path: file_path} = setting, options) when is_binary(file_path) do
    file_path |> Path.dirname() |> File.mkdir_p!()

    tid =
      if File.regular?(file_path) do
        Logger.info("Loading ets from file: #{file_path} ...")
        tid = Utils.setup_ets_from_file!(file_path)
        Logger.info("Loading ets from file: #{file_path} finished.")
        tid
      else
        :ets.new(__MODULE__, [:set, :public, write_concurrency: true, read_concurrency: true])
      end

    storage_context =
      Keyword.get(options, :context, %{})
      |> Map.get(:storage_context, %{})
      |> Map.merge(setting)
      |> Map.put(:tid, tid)

    {storage_context, options}
  end

  def prepare_for_start(file_path, options) when is_binary(file_path) do
    prepare_for_start(%{file_path: file_path}, options)
  end

  def prepare_for_start(arg, options) do
    Map.new(arg || [])
    |> Map.put_new_lazy(:file_path, fn ->
      Keyword.fetch!(options, :spider) |> Utils.get_file_path_by_spider("ets")
    end)
    |> prepare_for_start(options)
  end

  @impl true
  def prepare_for_stop(options) do
    %{storage_context: storage_context} = options[:context]
    %{file_path: file_path, tid: tid} = storage_context
    Logger.notice("starting dump ets to file: #{file_path} ...")
    result = Utils.dump_ets2file(tid, file_path)
    Logger.notice("dump ets to file: #{file_path} finished, result: #{inspect(result)}.")

    case storage_context[:give_away] do
      {pid, gift_msg} -> :ets.give_away(tid, pid, gift_msg)
      _ -> :ets.delete(tid)
    end

    :ok
  end
end
