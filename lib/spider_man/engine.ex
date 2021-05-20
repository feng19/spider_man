defmodule SpiderMan.Engine do
  @moduledoc false
  use GenServer
  require Logger

  alias SpiderMan.{
    Component.Downloader,
    Component.Spider,
    Component.ItemProcessor,
    Pipeline,
    Requester,
    Storage,
    Utils
  }

  @type state :: map
  @components [:downloader, :spider, :item_processor]

  def child_spec(settings) do
    %{
      id: Keyword.fetch!(settings, :spider),
      start: {__MODULE__, :start_link, [settings]},
      shutdown: 60_000
    }
  end

  def start_link(options) do
    spider = Keyword.fetch!(options, :spider)
    GenServer.start_link(__MODULE__, options, name: spider)
  end

  def status(spider), do: GenServer.call(spider, :status)
  defdelegate get_state(spider), to: :sys
  def suspend(spider, timeout \\ :infinity), do: GenServer.call(spider, :suspend, timeout)
  def suspend_component(spider, component, timeout \\ :infinity)

  def suspend_component(spider, component, timeout) when component in @components do
    GenServer.call(spider, {:suspend_component, component}, timeout)
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
    GenServer.call(spider, {:dump2file, file_name}, timeout)
  end

  def continue(spider, timeout \\ :infinity), do: GenServer.call(spider, :continue, timeout)
  def continue_component(spider, component, timeout \\ :infinity)

  def continue_component(spider, component, timeout) when component in @components do
    GenServer.call(spider, {:continue_component, component}, timeout)
  end

  def continue_component(_spider, _component, _timeout), do: :unknown_component

  def retry_failed(spider, max_retries \\ 3, timeout \\ :infinity) do
    GenServer.call(spider, {:retry_failed, max_retries}, timeout)
  end

  @impl true
  def init(options) do
    Process.flag(:trap_exit, true)
    state = Map.new(options)
    %{spider: spider, spider_module: spider_module} = state
    log_prefix = "!! spider: #{inspect(spider)},"
    Logger.metadata(spider: spider)
    log_file_path = configure_file_logger(spider, state[:log2file])
    Logger.info("#{log_prefix} configure_file_logger file: #{log_file_path} finish.")
    Logger.info("#{log_prefix} setup starting with spider_module: #{spider_module}.")
    status = Map.get(state, :status, :running)

    state =
      Map.merge(state, %{
        status: status,
        log_prefix: log_prefix,
        log_file_path: log_file_path
      })

    is_prepare_for_start? = function_exported?(spider_module, :prepare_for_start, 2)

    state =
      if is_prepare_for_start? do
        spider_module.prepare_for_start(:pre, state)
      else
        state
      end

    state =
      state
      |> setup_ets_tables()
      |> setup_components()

    state =
      if is_prepare_for_start? do
        spider_module.prepare_for_start(:post, state)
      else
        state
      end

    Logger.info("#{log_prefix} setup prepare_for_start finish.")

    state =
      if function_exported?(spider_module, :init, 1) do
        spider_module.init(state)
      else
        state
      end

    Logger.info("#{log_prefix} setup init finish.")
    Logger.info("#{log_prefix} setup success, status: #{status}.")
    {:ok, state}
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
        # common ets
        {"failed", state.failed_tid},
        {"common_pipeline", state.common_pipeline_tid},
        # producer ets
        {"downloader", state.downloader_tid},
        {"spider", state.spider_tid},
        {"item_processor", state.item_processor_tid},
        # pipeline ets
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

  def handle_call(
        {:retry_failed, max_retries},
        _from,
        %{
          failed_tid: failed_tid,
          downloader_tid: downloader_tid,
          spider_tid: spider_tid,
          item_processor_tid: item_processor_tid
        } = state
      ) do
    n =
      Utils.ets_stream(failed_tid)
      |> Enum.reduce(0, fn {k = {component, key}, data}, acc ->
        component_tid =
          case component do
            :downloader -> downloader_tid
            :spider -> spider_tid
            :item_processor -> item_processor_tid
          end

        :ets.insert(component_tid, {key, %{data | retries: max_retries}})
        :ets.delete(failed_tid, k)
        acc + 1
      end)

    {:reply, {:ok, n}, state}
  end

  def handle_call(msg, _from, state) do
    Logger.warn("unsupported call msg: #{msg}.")
    {:reply, :upsupported, state}
  end

  @impl true
  def handle_info({:EXIT, supervisor_pid, reason}, %{supervisor_pid: supervisor_pid} = state) do
    {:stop, reason, state}
  end

  @impl true
  def terminate(reason, state) do
    # wait all broadway stopped then go continue
    %{supervisor_pid: supervisor_pid, spider_module: spider_module, log_prefix: log_prefix} =
      state

    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, reason_to_signal(reason))

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end

    level = if reason in [:normal, :shutdown], do: :info, else: :error
    Logger.log(level, "#{log_prefix} terminate by reason: #{inspect(reason)}.")

    # prepare_for_stop_component
    @components
    |> Enum.zip([state.downloader_options, state.spider_options, state.item_processor_options])
    |> Enum.each(fn {component, options} ->
      prepare_for_stop_component(component, options, spider_module, log_prefix)
    end)

    Logger.log(level, "#{log_prefix} components prepare_for_stop finish.")

    Requester.prepare_for_stop(state.downloader_options)
    Logger.log(level, "#{log_prefix} Requester prepare_for_stop finish.")

    Storage.prepare_for_stop(state.item_processor_options)
    Logger.log(level, "#{log_prefix} Storage prepare_for_stop finish.")

    # prepare_for_stop
    if function_exported?(spider_module, :prepare_for_stop, 1) do
      spider_module.prepare_for_stop(state)
    end

    Logger.log(level, "#{log_prefix} prepare_for_stop finish.")
    Logger.log(level, "#{log_prefix} Engine stopped.")
    :ok
  end

  defp reason_to_signal(:killed), do: :kill
  defp reason_to_signal(other), do: other

  defp prepare_for_start_component(options, component, spider_module, log_prefix) do
    options =
      if options do
        {pipelines, options} = Pipeline.prepare_for_start(options[:pipelines], options)
        {post_pipelines, options} = Pipeline.prepare_for_start(options[:post_pipelines], options)

        Logger.info(
          "#{log_prefix} #{component} component pipelines setup prepare_for_start finish."
        )

        context =
          Map.merge(options[:context], %{pipelines: pipelines, post_pipelines: post_pipelines})

        Keyword.put(options, :context, context)
      else
        options
      end

    options =
      if function_exported?(spider_module, :prepare_for_start_component, 2) do
        options = spider_module.prepare_for_start_component(component, options)

        Logger.info(
          "#{log_prefix} setup #{component} component prepare_for_start_component finish."
        )

        options
      else
        options
      end
      |> SpiderMan.Producer.prepare_for_start_producer()

    Logger.info("#{log_prefix} #{component} component prepare_for_start finish.")
    options
  end

  defp prepare_for_stop_component(component, options, spider_module, log_prefix) do
    if options do
      options |> Keyword.fetch!(:pipelines) |> Pipeline.prepare_for_stop()
      options |> Keyword.fetch!(:post_pipelines) |> Pipeline.prepare_for_stop()
    end

    Logger.info("#{log_prefix} #{component} component pipelines prepare_for_stop finish.")

    if function_exported?(spider_module, :prepare_for_stop_component, 2) do
      spider_module.prepare_for_stop_component(component, options)
      Logger.info("#{log_prefix} #{component} component prepare_for_stop_component finish.")
    end

    Logger.info("#{log_prefix} #{component} component prepare_for_stop finish.")
  end

  defp call_producers(state, msg) do
    Enum.map(
      [state.downloader_pid, state.spider_pid, state.item_processor_pid],
      &Utils.call_producer(&1, msg)
    )
  end

  defp setup_ets_tables(%{spider: spider, log_prefix: log_prefix} = state) do
    state = do_setup_ets_tables(state)

    :persistent_term.put(spider, %{
      failed_tid: state.failed_tid,
      common_pipeline_tid: state.common_pipeline_tid,
      downloader_tid: state.downloader_tid,
      spider_tid: state.spider_tid,
      item_processor_tid: state.item_processor_tid
    })

    Logger.info("#{log_prefix} setup ets tables finish.")
    state
  end

  defp do_setup_ets_tables(%{ets_file: file_name, log_prefix: log_prefix} = state) do
    Logger.info("#{log_prefix} starting setup_ets_from_file: #{file_name}_*.ets ...")

    ets_tables =
      Map.new(
        [
          # common ets
          failed_tid: "failed",
          common_pipeline_tid: "common_pipeline",
          # producer ets
          downloader_tid: "downloader",
          spider_tid: "spider",
          item_processor_tid: "item_processor",
          # pipeline ets
          downloader_pipeline_tid: "downloader_pipeline",
          spider_pipeline_tid: "spider_pipeline",
          item_processor_pipeline_tid: "item_processor_pipeline"
        ],
        fn {key, file_suffix} ->
          tid = setup_ets_from_file!("#{file_name}_#{file_suffix}.ets", log_prefix)
          {key, tid}
        end
      )

    Logger.info("#{log_prefix} setup_ets_from_file: #{file_name}_*.ets finished.")

    Map.merge(state, ets_tables)
  end

  defp do_setup_ets_tables(state) do
    # new ets tables
    producer_ets_options = [:set, :public, write_concurrency: true]
    ets_options = [:set, :public, write_concurrency: true, read_concurrency: true]

    ets_tables = %{
      # common ets
      failed_tid: :ets.new(:failed, ets_options),
      common_pipeline_tid: :ets.new(:common_pipeline, ets_options),
      # producer ets
      downloader_tid: :ets.new(:downloader, producer_ets_options),
      spider_tid: :ets.new(:spider, producer_ets_options),
      item_processor_tid: :ets.new(:item_processor, producer_ets_options),
      # pipeline ets
      downloader_pipeline_tid: :ets.new(:downloader_pipeline, ets_options),
      spider_pipeline_tid: :ets.new(:spider_pipeline, ets_options),
      item_processor_pipeline_tid: :ets.new(:item_processor_pipeline, ets_options)
    }

    Map.merge(state, ets_tables)
  end

  defp do_dump2file(file_name, tid) do
    Logger.notice("starting dump2file: #{file_name} ...")
    file_name = String.to_charlist(file_name)
    result = :ets.tab2file(tid, file_name, extended_info: [:md5sum], sync: true)
    Logger.notice("dump2file: #{file_name} finished, result: #{inspect(result)}.")
  end

  defp setup_ets_from_file!(file_name, log_prefix) do
    Logger.info("#{log_prefix} loading from file: #{file_name} ...")

    file_name
    |> String.to_charlist()
    |> :ets.file2tab(verify: true)
    |> case do
      {:ok, tid} -> tid
      {:error, error} -> raise "setup_ets_from_file: #{file_name} error: #{inspect(error)}"
    end
  end

  defp setup_components(
         %{
           spider: spider,
           spider_module: spider_module,
           status: status,
           log_prefix: log_prefix,
           # common ets
           common_pipeline_tid: common_pipeline_tid,
           failed_tid: failed_tid,
           # producer ets
           downloader_tid: downloader_tid,
           spider_tid: spider_tid,
           item_processor_tid: item_processor_tid,
           # pipeline ets
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
      common_pipeline_tid: common_pipeline_tid,
      failed_tid: failed_tid
    ]

    downloader_options =
      [
        component: :downloader,
        tid: downloader_tid,
        next_tid: spider_tid,
        pipeline_tid: downloader_pipeline_tid
      ]
      |> Kernel.++(broadway_base_options)
      |> Kernel.++(state.downloader_options)
      |> Requester.prepare_for_start()
      |> prepare_for_start_component(:downloader, spider_module, log_prefix)

    spider_options =
      [
        component: :spider,
        tid: spider_tid,
        next_tid: item_processor_tid,
        pipeline_tid: spider_pipeline_tid
      ]
      |> Kernel.++(broadway_base_options)
      |> Kernel.++(state.spider_options)
      |> prepare_for_start_component(:spider, spider_module, log_prefix)

    item_processor_options =
      [
        component: :item_processor,
        tid: item_processor_tid,
        next_tid: nil,
        pipeline_tid: item_processor_pipeline_tid
      ]
      |> Kernel.++(broadway_base_options)
      |> Kernel.++(state.item_processor_options)
      |> Storage.prepare_for_start()
      |> prepare_for_start_component(:item_processor, spider_module, log_prefix)

    Logger.info("#{log_prefix} setup components prepare_for_start finish.")

    Map.merge(state, %{
      # options
      downloader_options: downloader_options,
      spider_options: spider_options,
      item_processor_options: item_processor_options
    })
    |> start_components()
  end

  defp start_components(state) do
    name = :"#{state.spider}.Supervisor"
    state = Map.merge(state, %{downloader_pid: nil, spider_pid: nil, item_processor_pid: nil})

    {components, specs} =
      [
        {:downloader_pid, {Downloader, state.downloader_options}},
        {:spider_pid, {Spider, state.spider_options}},
        {:item_processor_pid, {ItemProcessor, state.item_processor_options}}
      ]
      |> Enum.filter(&match?({_, {_, options}} when is_list(options), &1))
      |> Enum.unzip()

    Supervisor.start_link(
      specs,
      strategy: :one_for_one,
      max_restarts: 0,
      name: name
    )
    |> case do
      {:ok, supervisor_pid} ->
        Logger.info("#{state.log_prefix} setup components finish.")

        component_pids =
          Supervisor.which_children(supervisor_pid) |> Enum.reduce([], &[elem(&1, 1) | &2])

        Enum.zip(components, component_pids)
        |> Enum.into(state)
        |> Map.put(:supervisor_pid, supervisor_pid)

      error ->
        raise "start name: #{name} error: #{inspect(error)}"
    end
  end

  defp get_component_pid(:downloader, %{downloader_pid: pid}), do: pid
  defp get_component_pid(:spider, %{spider_pid: pid}), do: pid
  defp get_component_pid(:item_processor, %{item_processor_pid: pid}), do: pid

  defp configure_file_logger(_spider, false), do: :skiped

  defp configure_file_logger(spider, log2file) when log2file in [nil, true] do
    timestamp = System.system_time(:second)
    log_file_path = Path.join([System.tmp_dir(), inspect(spider), "#{timestamp}.log"])
    configure_file_logger(spider, log_file_path)
  end

  defp configure_file_logger(spider, log_file_path) do
    backend = {LoggerFileBackend, spider}
    Logger.add_backend(backend)

    Logger.configure_backend(backend,
      path: log_file_path,
      level: :debug,
      metadata_filter: [spider: spider]
    )

    log_file_path
  end
end
