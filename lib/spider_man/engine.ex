defmodule SpiderMan.Engine do
  @moduledoc false
  use GenServer, shutdown: 60_000
  require Logger
  alias SpiderMan.{Downloader, Spider, ItemProcessor, Pipeline, Utils}

  @type state :: map
  @components [:downloader, :spider, :item_processor]

  def process_name(spider), do: :"#{spider}.Engine"

  def start_link(options) do
    spider = Keyword.fetch!(options, :spider)
    GenServer.start_link(__MODULE__, options, name: process_name(spider))
  end

  def status(spider), do: call(spider, :status)
  def get_state(spider), do: spider |> process_name() |> :sys.get_state()
  def suspend(spider, timeout \\ :infinity), do: call(spider, :suspend, timeout)
  def suspend_component(spider, component, timeout \\ :infinity)

  def suspend_component(spider, component, timeout) when component in @components do
    call(spider, {:suspend_component, component}, timeout)
  end

  def suspend_component(_spider, _component, _timeout), do: :unknown_component

  def dump2file(spider, file_name \\ nil, timeout \\ :infinity) do
    Logger.notice("Please ensure all producer's events is save done before dump2file: Y/N?")

    case IO.read(1) do
      "Y" ->
        dump2file_force(spider, file_name, timeout)

      _ ->
        Logger.notice("Canceled!!!")
    end
  end

  def dump2file_force(spider, file_name \\ nil, timeout \\ :infinity) do
    call(spider, {:dump2file, file_name}, timeout)
  end

  def continue(spider, timeout \\ :infinity), do: call(spider, :continue, timeout)
  def continue_component(spider, component, timeout \\ :infinity)

  def continue_component(spider, component, timeout) when component in @components do
    call(spider, {:continue_component, component}, timeout)
  end

  def continue_component(_spider, _component, _timeout), do: :unknown_component

  defp call(spider, msg, timeout \\ 5_000) do
    spider
    |> process_name()
    |> GenServer.call(msg, timeout)
  end

  @impl true
  def init(options) do
    state = Map.new(options)
    spider = state.spider
    log_prefix = "!! spider: #{inspect(spider)},"
    Logger.info("#{log_prefix} setup starting with spider_module: #{state.spider_module}.")
    Process.flag(:trap_exit, true)
    component_sup = SpiderMan.Component.Supervisor.process_name(spider)
    continue_status = Map.get(state, :status, :running)

    state =
      Map.merge(state, %{
        status: :preparing,
        continue_status: continue_status,
        component_sup: component_sup,
        log_prefix: log_prefix
      })

    {:ok, state, {:continue, :start_components}}
  end

  @impl true
  def handle_continue(:start_components, state) do
    %{
      spider_module: spider_module,
      log_prefix: log_prefix,
      component_sup: component_sup,
      continue_status: continue_status
    } = state

    is_prepare_for_start? = function_exported?(spider_module, :prepare_for_start, 2)

    state =
      if is_prepare_for_start? do
        spider_module.prepare_for_start(:pre, state)
      else
        state
      end

    component_sup = wait_component_sup_started(component_sup)
    Logger.info("#{log_prefix} setup Component.Supervisor finish.")

    state =
      state
      |> Map.merge(%{status: continue_status, component_sup: component_sup})
      |> Map.delete(:continue_status)
      |> setup_ets_tables()
      |> setup_components()

    state =
      if is_prepare_for_start? do
        spider_module.prepare_for_start(:post, state)
      else
        state
      end

    Logger.info("#{log_prefix} setup prepare_for_start finish.")
    Logger.info("#{log_prefix} setup success, status: #{continue_status}.")
    {:noreply, state}
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  def handle_call(:suspend, _from, %{status: :running, log_prefix: log_prefix} = state) do
    [:ok, :ok, :ok] = call_producers(state, :suspend)
    Logger.info("#{log_prefix} status turn to suspend.")
    {:reply, :ok, %{state | status: :suspended}}
  end

  def handle_call(:suspend, _from, state), do: {:reply, :ok, state}

  def handle_call({:suspend_component, component}, _from, state) do
    result =
      component
      |> get_component_pid(state)
      |> Utils.call_producer(:suspend)

    {:reply, result, state}
  end

  def handle_call(:continue, _from, %{status: :suspended, log_prefix: log_prefix} = state) do
    [:ok, :ok, :ok] = call_producers(state, :continue)
    Logger.info("#{log_prefix} status turn to running.")
    {:reply, :ok, %{state | status: :running}}
  end

  def handle_call(:continue, _from, state), do: {:reply, :ok, state}

  def handle_call({:continue_component, component}, _from, state) do
    result =
      component
      |> get_component_pid(state)
      |> Utils.call_producer(:continue)

    {:reply, result, state}
  end

  def handle_call(
        {:dump2file, file_name},
        _from,
        %{status: :suspended, spider: spider, log_prefix: log_prefix} = state
      ) do
    file_name = file_name || "./data/#{inspect(spider)}_#{System.system_time(:second)}"
    Path.dirname(file_name) |> File.mkdir_p()
    Logger.notice("#{log_prefix} starting dump2file: #{file_name}_*.ets ...")

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

    Logger.notice("#{log_prefix} dump2file: #{file_name}_*.ets finished.")
    {:reply, :ok, state}
  end

  def handle_call({:dump2file, _}, _from, state), do: {:reply, :status_error, state}

  def handle_call(msg, _from, state) do
    Logger.warn("unsupported call msg: #{msg}.")
    {:reply, :upsupported, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, state),
    do: {:stop, :shutdown, state}

  def handle_info({:DOWN, _ref, :process, pid, reason}, %{component_sup: pid} = state),
    do: {:stop, {:component_down, reason}, state}

  @impl true
  def terminate(reason, state) do
    %{spider_module: spider_module, log_prefix: log_prefix} = state
    level = if reason in [:normal, :shutdown], do: :info, else: :warning
    Logger.log(level, "#{log_prefix} terminate by reason: #{inspect(reason)}.")

    # prepare_for_stop_component
    @components
    |> Enum.zip([state.downloader_options, state.spider_options, state.item_processor_options])
    |> Enum.each(fn {component, options} ->
      prepare_for_stop_component(component, options, spider_module)
      Logger.info("#{log_prefix} #{component} component prepare_for_stop_pipelines finish.")
    end)

    Logger.log(level, "#{log_prefix} prepare_for_stop_component finish.")

    # prepare_for_stop
    if function_exported?(spider_module, :prepare_for_stop, 1) do
      spider_module.prepare_for_stop(state)
    end

    Logger.log(level, "#{log_prefix} prepare_for_stop finish.")
    Logger.log(level, "#{log_prefix} stop finish.")

    :ok
  end

  defp prepare_for_start_component(options, component, spider_module) do
    if function_exported?(spider_module, :prepare_for_start_component, 2) do
      spider_module.prepare_for_start_component(component, options)
    else
      options
    end
  end

  defp prepare_for_stop_component(component, options, spider_module) do
    if function_exported?(spider_module, :prepare_for_stop_component, 2) do
      spider_module.prepare_for_stop_component(component, options)
    end

    options
    |> Keyword.fetch!(:pipelines)
    |> Pipeline.prepare_for_stop()
  end

  defp setup_requester(options) do
    {requester, arg} =
      case Keyword.get(options, :requester, SpiderMan.Requester.Finch) do
        {requester, _arg} = r when is_atom(requester) -> r
        requester when is_atom(requester) -> {requester, []}
      end

    options =
      Keyword.update(
        options,
        :context,
        %{requester: requester},
        &Map.put(&1, :requester, requester)
      )

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

  defp setup_ets_tables(%{spider: spider, log_prefix: log_prefix} = state) do
    state = do_setup_ets_tables(state)
    :persistent_term.put({spider, :downloader_tid}, state.downloader_tid)
    :persistent_term.put({spider, :spider_tid}, state.spider_tid)
    :persistent_term.put({spider, :item_processor_tid}, state.item_processor_tid)
    :persistent_term.put({spider, :common_pipeline_tid}, state.common_pipeline_tid)
    Logger.info("#{log_prefix} setup ets tables finish.")
    state
  end

  defp do_setup_ets_tables(%{load_from_file: file_name, log_prefix: log_prefix} = state) do
    Logger.info("#{log_prefix} starting load_from_file: #{file_name}_*.ets ...")

    ets_tables =
      Map.new(
        [
          downloader_tid: "downloader",
          spider_tid: "spider",
          item_processor_tid: "item_processor",
          common_pipeline_tid: "common_pipeline",
          downloader_pipeline_tid: "downloader_pipeline",
          spider_pipeline_tid: "spider_pipeline",
          item_processor_pipeline_tid: "item_processor_pipeline"
        ],
        fn {key, file_suffix} ->
          tid = do_load_from_file!("#{file_name}_#{file_suffix}.ets", log_prefix)
          {key, tid}
        end
      )

    Logger.info("#{log_prefix} load_from_file: #{file_name}_*.ets finished.")

    Map.merge(state, ets_tables)
  end

  defp do_setup_ets_tables(state) do
    # new ets tables
    ets_options = [:set, :public, write_concurrency: true]
    pipeline_ets_options = [:set, :public, write_concurrency: true, read_concurrency: true]

    ets_tables = %{
      downloader_tid: :ets.new(:downloader, ets_options),
      spider_tid: :ets.new(:spider, ets_options),
      item_processor_tid: :ets.new(:item_processor, ets_options),
      common_pipeline_tid: :ets.new(:common_pipeline, pipeline_ets_options),
      downloader_pipeline_tid: :ets.new(:downloader_pipeline, pipeline_ets_options),
      spider_pipeline_tid: :ets.new(:spider_pipeline, pipeline_ets_options),
      item_processor_pipeline_tid: :ets.new(:item_processor_pipeline, pipeline_ets_options)
    }

    Map.merge(state, ets_tables)
  end

  defp do_dump2file(file_name, tid) do
    Logger.notice("starting dump2file: #{file_name} ...")
    file_name = String.to_charlist(file_name)
    result = :ets.tab2file(tid, file_name, extended_info: [:md5sum], sync: true)
    Logger.notice("dump2file: #{file_name} finished, result: #{inspect(result)}.")
  end

  defp do_load_from_file!(file_name, log_prefix) do
    Logger.info("#{log_prefix} loading from file: #{file_name} ...")

    file_name
    |> String.to_charlist()
    |> :ets.file2tab(verify: true)
    |> case do
      {:ok, tid} -> tid
      {:error, error} -> raise "load_from_file: #{file_name} error: #{inspect(error)}"
    end
  end

  defp wait_component_sup_started(component_sup) do
    with pid when pid != nil <- Process.whereis(component_sup),
         true <- Process.alive?(pid) do
      Process.monitor(component_sup)
      pid
    else
      _ ->
        Process.sleep(100)
        wait_component_sup_started(component_sup)
    end
  end

  defp setup_components(
         %{
           spider: spider,
           spider_module: spider_module,
           status: status,
           log_prefix: log_prefix,
           component_sup: component_sup,
           downloader_tid: downloader_tid,
           spider_tid: spider_tid,
           item_processor_tid: item_processor_tid,
           common_pipeline_tid: common_pipeline_tid,
           downloader_pipeline_tid: downloader_pipeline_tid,
           spider_pipeline_tid: spider_pipeline_tid,
           item_processor_pipeline_tid: item_processor_pipeline_tid
         } = state
       ) do
    # setup component's options
    broadway_base_options = [
      spider: spider,
      spider_module: spider_module,
      status: status,
      common_pipeline_tid: common_pipeline_tid
    ]

    downloader_options =
      [
        tid: downloader_tid,
        next_tid: spider_tid,
        pipeline_tid: downloader_pipeline_tid
      ]
      |> Kernel.++(broadway_base_options)
      |> Kernel.++(state.downloader_options)
      |> setup_requester()
      |> prepare_for_start_component(:downloader, spider_module)

    spider_options =
      [
        tid: spider_tid,
        next_tid: item_processor_tid,
        pipeline_tid: spider_pipeline_tid
      ]
      |> Kernel.++(broadway_base_options)
      |> Kernel.++(state.spider_options)
      |> prepare_for_start_component(:spider, spider_module)

    item_processor_options =
      [tid: item_processor_tid, pipeline_tid: item_processor_pipeline_tid]
      |> Kernel.++(broadway_base_options)
      |> Kernel.++(state.item_processor_options)
      |> setup_item_processor_context()
      |> prepare_for_start_component(:item_processor, spider_module)

    Logger.info("#{log_prefix} setup prepare_for_start_component finish.")

    # start components
    [downloader_pid, spider_pid, item_processor_pid] =
      start_components(component_sup, [
        {Downloader, downloader_options},
        {Spider, spider_options},
        {ItemProcessor, item_processor_options}
      ])

    Logger.info("#{log_prefix} setup components finish.")

    Map.merge(state, %{
      # options
      downloader_options: downloader_options,
      spider_options: spider_options,
      item_processor_options: item_processor_options,
      # components
      downloader_pid: downloader_pid,
      spider_pid: spider_pid,
      item_processor_pid: item_processor_pid
    })
  end

  defp start_components(_component_sup, []), do: []

  defp start_components(component_sup, [child | children]) do
    {:ok, pid} = Supervisor.start_child(component_sup, child)
    [pid | start_components(component_sup, children)]
  catch
    # ensure successfully start component when restarting engine
    :error, {:badmatch, {:error, {{{:badmatch, {:error, {:already_started, _pid}}}, _}, _}}} ->
      Process.sleep(100)
      start_components(component_sup, [child | children])
  end

  defp get_component_pid(:downloader, %{downloader_pid: pid}), do: pid
  defp get_component_pid(:spider, %{spider_pid: pid}), do: pid
  defp get_component_pid(:item_processor, %{item_processor_pid: pid}), do: pid
end
