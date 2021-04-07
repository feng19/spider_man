defmodule SpiderMan.Engine do
  @moduledoc false
  use GenServer, shutdown: 60_000
  require Logger
  alias SpiderMan.{Downloader, Spider, ItemProcessor, Pipeline, Utils}

  @type state :: map

  def process_name(spider), do: :"#{inspect(spider)}.Engine"

  def start_link(options) do
    spider = Keyword.fetch!(options, :spider)
    GenServer.start_link(__MODULE__, options, name: process_name(spider))
  end

  def status(spider), do: GenServer.call(process_name(spider), :status)

  def suspend(spider, timeout \\ :infinity) do
    process_name(spider)
    |> GenServer.call(:suspend, timeout)
  end

  def dump2file(spider, file_name \\ nil, timeout \\ :infinity) do
    IO.puts("Please ensure all producer's events is save done before dump2file: Y/N?")

    case IO.read(1) do
      "Y" ->
        file_name = file_name || "./data/#{inspect(spider)}_#{System.system_time(:second)}"
        IO.puts("starting dump2file: #{file_name}_*.ets ...")

        result =
          process_name(spider)
          |> GenServer.call({:dump2file, file_name}, timeout)

        IO.puts("dump2file: #{file_name}_*.ets finished, result: #{result}.")

      _ ->
        IO.puts("Canceled!!!")
    end
  end

  def continue(spider, timeout \\ :infinity) do
    process_name(spider)
    |> GenServer.call(:continue, timeout)
  end

  @impl true
  def init(options) do
    state = Map.new(options) |> Map.put(:status, :preparing)
    Logger.info("!! spider: #{inspect(state.spider)} setup starting.")
    Process.flag(:trap_exit, true)

    {:ok, state, {:continue, :start_components}}
  end

  @impl true
  def handle_continue(:start_components, state) do
    state = setup_ets_tables(state)

    %{
      spider: spider,
      downloader_tid: downloader_tid,
      spider_tid: spider_tid,
      item_processor_tid: item_processor_tid,
      common_pipeline_tid: common_pipeline_tid,
      downloader_pipeline_tid: downloader_pipeline_tid,
      spider_pipeline_tid: spider_pipeline_tid,
      item_processor_pipeline_tid: item_processor_pipeline_tid
    } = state

    :persistent_term.put({spider, :common_pipeline_tid}, common_pipeline_tid)
    :persistent_term.put({spider, :downloader_tid}, downloader_tid)
    :persistent_term.put({spider, :spider_tid}, spider_tid)
    :persistent_term.put({spider, :item_processor_tid}, item_processor_tid)

    Logger.info("!! spider: #{inspect(spider)} setup ets tables finish.")

    # setup component's options
    downloader_options =
      [
        spider: spider,
        tid: downloader_tid,
        next_tid: spider_tid,
        common_pipeline_tid: common_pipeline_tid,
        pipeline_tid: downloader_pipeline_tid
      ]
      |> Kernel.++(state.downloader_options)
      |> setup_requester()
      |> prepare_for_start_component(:downloader, spider)

    spider_options =
      [
        spider: spider,
        tid: spider_tid,
        next_tid: item_processor_tid,
        common_pipeline_tid: common_pipeline_tid,
        pipeline_tid: spider_pipeline_tid
      ]
      |> Kernel.++(state.spider_options)
      |> prepare_for_start_component(:spider, spider)

    item_processor_options =
      [
        spider: spider,
        tid: item_processor_tid,
        common_pipeline_tid: common_pipeline_tid,
        pipeline_tid: item_processor_pipeline_tid
      ]
      |> Kernel.++(state.item_processor_options)
      |> setup_item_processor_context()
      |> prepare_for_start_component(:item_processor, spider)

    Logger.info("!! spider: #{inspect(spider)} setup prepare_for_start_component finish.")

    # start components
    {:ok, downloader_pid} = Supervisor.start_child(spider, {Downloader, downloader_options})
    {:ok, spider_pid} = Supervisor.start_child(spider, {Spider, spider_options})

    {:ok, item_processor_pid} =
      Supervisor.start_child(spider, {ItemProcessor, item_processor_options})

    Logger.info("!! spider: #{inspect(spider)} setup components finish.")

    state =
      Map.merge(state, %{
        status: :running,
        # options
        downloader_options: downloader_options,
        spider_options: spider_options,
        item_processor_options: item_processor_options,
        # broadways
        downloader_pid: downloader_pid,
        spider_pid: spider_pid,
        item_processor_pid: item_processor_pid
      })

    state =
      if function_exported?(spider, :prepare_for_start, 1) do
        spider.prepare_for_start(state)
      else
        state
      end

    Logger.info("!! spider: #{inspect(spider)} setup prepare_for_start finish.")

    Logger.info("!! spider: #{inspect(spider)} setup success.")
    {:noreply, state}
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  def handle_call(:suspend, _from, %{status: :running} = state) do
    [:ok, :ok, :ok] = call_producers(state, :suspend)
    {:reply, :ok, %{state | status: :suspend}}
  end

  def handle_call(:suspend, _from, state), do: {:reply, :ok, state}

  def handle_call(:continue, _from, %{status: :suspend} = state) do
    [:ok, :ok, :ok] = call_producers(state, :continue)
    {:reply, :ok, %{state | status: :running}}
  end

  def handle_call(:continue, _from, state), do: {:reply, :ok, state}

  def handle_call({:dump2file, file_name}, _from, %{status: :suspend} = state) do
    Enum.each(
      [
        {"downloader", state.downloader_tid},
        {"spider", state.spider_tid},
        {"item_processor", state.item_processor_tid},
        {"common_pipeline", state.common_pipeline_tid},
        {"downloader_pipeline", state.downloader_pipeline_tid},
        {"spider_pipeline", state.spider_pipeline_tid},
        {"item_processor_pipeline", state.item_processor_pipeline_tid}
      ],
      fn {name, tid} -> do_dump2file("#{file_name}_#{name}.ets", tid) end
    )

    {:reply, :ok, state}
  end

  def handle_call({:dump2file, _}, _from, state), do: {:reply, :status_error, state}

  def handle_call(msg, _from, state) do
    Logger.warn("unsupported call msg: #{msg}.")
    {:reply, :upsupported, state}
  end

  @impl true
  def terminate(reason, state) do
    spider = state.spider
    level = if reason == :normal, do: :info, else: :warning
    Logger.log(level, "!! spider: #{inspect(spider)} terminate by reason: #{inspect(reason)}.")

    # prepare_for_stop
    prepare_for_stop_component(:downloader, state.downloader_options, spider)
    prepare_for_stop_component(:spider, state.spider_options, spider)
    prepare_for_stop_component(:item_processor, state.item_processor_options, spider)

    if function_exported?(spider, :prepare_for_stop, 1) do
      spider.prepare_for_stop(state)
    end

    Logger.log(level, "!! spider: #{inspect(spider)} prepare_for_stop finish.")

    Task.async(fn ->
      :ok = Supervisor.stop(spider, reason)
      Logger.log(level, "!! spider: #{inspect(spider)} stop finish.")
    end)

    :ok
  end

  defp prepare_for_start_component(options, component, spider) do
    if function_exported?(spider, :prepare_for_start_component, 2) do
      spider.prepare_for_start_component(component, options)
    else
      options
    end
  end

  defp prepare_for_stop_component(component, options, spider) do
    if function_exported?(spider, :prepare_for_stop_component, 2) do
      spider.prepare_for_stop_component(component, options)
    end

    options
    |> Keyword.fetch!(:pipelines)
    |> Pipeline.prepare_for_stop()

    Logger.info(
      "!! spider: #{inspect(spider)}, component: #{inspect(component)} setup prepare_for_stop_pipelines finish."
    )
  end

  defp setup_requester(options) do
    {requester, arg} =
      case Keyword.get(options, :requester, SpiderMan.Requester.Finch) do
        {requester, _arg} = r when is_atom(requester) -> r
        requester when is_atom(requester) -> {requester, []}
      end

    with {:module, _} <- Code.ensure_loaded(requester),
         true <- function_exported?(requester, :prepare_for_start, 2) do
      requester.prepare_for_start(arg, options)
    else
      _ -> options
    end
  end

  defp setup_item_processor_context(options) do
    {storage, arg} =
      case Keyword.get(options, :storage, SpiderMan.Storage.Log) do
        {storage, _arg} = r when is_atom(storage) -> r
        storage when is_atom(storage) -> {storage, []}
      end

    options =
      with {:module, _} <- Code.ensure_loaded(storage),
           true <- function_exported?(storage, :prepare_for_start, 2) do
        storage.prepare_for_start(arg, options)
      else
        _ -> options
      end

    storage_options = Keyword.get(options, :storage_options, [])

    context =
      options
      |> Keyword.get(:context, %{})
      |> Map.merge(%{storage: storage, storage_options: storage_options})

    Keyword.put(options, :context, context)
  end

  defp call_producers(state, msg) do
    Enum.map(
      [state.downloader_pid, state.spider_pid, state.item_processor_pid],
      &Utils.call_producer(&1, msg)
    )
  end

  defp setup_ets_tables(%{load_from_file: file_name, spider: spider} = state) do
    Logger.info("!! spider: #{inspect(spider)} starting load_from_file: #{file_name}_*.ets ...")

    [
      downloader_tid,
      spider_tid,
      item_processor_tid,
      common_pipeline_tid,
      downloader_pipeline_tid,
      spider_pipeline_tid,
      item_processor_pipeline_tid
    ] =
      Enum.map(
        [
          "downloader",
          "spider",
          "item_processor",
          "common_pipeline",
          "downloader_pipeline",
          "spider_pipeline",
          "item_processor_pipeline"
        ],
        &do_load_from_file!("#{file_name}_#{&1}.ets")
      )

    Logger.info("!! spider: #{inspect(spider)} load_from_file: #{file_name}_*.ets finished.")

    Map.merge(state, %{
      downloader_tid: downloader_tid,
      spider_tid: spider_tid,
      item_processor_tid: item_processor_tid,
      common_pipeline_tid: common_pipeline_tid,
      downloader_pipeline_tid: downloader_pipeline_tid,
      spider_pipeline_tid: spider_pipeline_tid,
      item_processor_pipeline_tid: item_processor_pipeline_tid
    })
  end

  defp setup_ets_tables(state) do
    # new ets tables
    ets_options = [:set, :public, write_concurrency: true]
    pipeline_ets_options = [:set, :public, write_concurrency: true, read_concurrency: true]
    downloader_tid = :ets.new(:downloader, ets_options)
    spider_tid = :ets.new(:spider, ets_options)
    item_processor_tid = :ets.new(:item_processor, ets_options)
    common_pipeline_tid = :ets.new(:common_pipeline, pipeline_ets_options)
    downloader_pipeline_tid = :ets.new(:downloader_pipeline, pipeline_ets_options)
    spider_pipeline_tid = :ets.new(:spider_pipeline, pipeline_ets_options)
    item_processor_pipeline_tid = :ets.new(:item_processor_pipeline, pipeline_ets_options)

    Map.merge(state, %{
      downloader_tid: downloader_tid,
      spider_tid: spider_tid,
      item_processor_tid: item_processor_tid,
      common_pipeline_tid: common_pipeline_tid,
      downloader_pipeline_tid: downloader_pipeline_tid,
      spider_pipeline_tid: spider_pipeline_tid,
      item_processor_pipeline_tid: item_processor_pipeline_tid
    })
  end

  defp do_dump2file(file_name, tid) do
    IO.puts("starting dump2file: #{file_name} ...")
    file_name = String.to_charlist(file_name)
    result = :ets.tab2file(tid, file_name, extended_info: [:md5sum], sync: true)
    IO.puts("dump2file: #{file_name} finished, result: #{inspect(result)}.")
  end

  defp do_load_from_file!(file_name) do
    file_name
    |> String.to_charlist()
    |> :ets.file2tab(verify: true)
    |> case do
      {:ok, tid} -> tid
      {:error, error} -> raise "load_from_file: #{file_name} error: #{inspect(error)}"
    end
  end
end
