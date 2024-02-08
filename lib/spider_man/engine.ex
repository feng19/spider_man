defmodule SpiderMan.Engine do
  @moduledoc "Engine"
  use GenServer
  require Logger

  alias SpiderMan.{
    Component.Downloader,
    Component.ItemProcessor,
    Component.Spider,
    Pipeline,
    Producer,
    Requester,
    Stats,
    Storage,
    Utils
  }

  @type state :: map
  @components [:downloader, :spider, :item_processor]
  @ets_key_filename_mapping [
    # common ets
    stats_tid: "stats",
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
  ]
  @ets_filename_key_mapping Enum.map(@ets_key_filename_mapping, fn {k, v} -> {v, k} end)

  @doc false
  def child_spec(settings) do
    %{
      id: Keyword.fetch!(settings, :spider),
      start: {__MODULE__, :start_link, [settings]},
      shutdown: 60_000
    }
  end

  @doc false
  def start_link(options) do
    spider = Keyword.fetch!(options, :spider)
    GenServer.start_link(__MODULE__, options, name: spider)
  end

  @doc false
  def status(spider), do: GenServer.call(spider, :status)
  @doc false
  defdelegate get_state(spider), to: :sys
  @doc false
  def suspend(spider, timeout \\ :infinity), do: GenServer.call(spider, :suspend, timeout)
  @doc false
  def suspend_component(spider, component, timeout \\ :infinity)

  def suspend_component(spider, component, timeout) when component in @components do
    GenServer.call(spider, {:suspend_component, component}, timeout)
  end

  def suspend_component(_spider, _component, _timeout), do: :unknown_component

  @doc "dump spider's ets tables to files"
  def dump2file(spider, file_name \\ nil, timeout \\ :infinity) do
    Logger.notice("Please ensure all producer's events is save done before dump2file: Y/N?")

    case IO.read(1) do
      "Y" ->
        dump2file_force(spider, file_name, timeout)

      _ ->
        Logger.notice("Canceled!!!")
    end
  end

  @doc "dump spider's ets tables to files, don't need confirm"
  def dump2file_force(spider, file_name \\ nil, timeout \\ :infinity) do
    GenServer.call(spider, {:dump2file, file_name}, timeout)
  end

  @doc false
  def continue(spider, timeout \\ :infinity), do: GenServer.call(spider, :continue, timeout)
  @doc false
  def continue_component(spider, component, timeout \\ :infinity)

  def continue_component(spider, component, timeout) when component in @components do
    GenServer.call(spider, {:continue_component, component}, timeout)
  end

  def continue_component(_spider, _component, _timeout), do: :unknown_component
  @doc false
  def retry_failed(spider, max_retries \\ 3, timeout \\ :infinity) do
    GenServer.call(spider, {:retry_failed, max_retries}, timeout)
  end

  @impl true
  def init(options) do
    {gl, options} = Keyword.pop(options, :group_leader)
    gl && Process.group_leader(self(), gl)
    Process.flag(:trap_exit, true)
    %{spider: spider, spider_module: spider_module} = state = Map.new(options)

    log_prefix = Map.get(state, :log_prefix, "!! spider: #{inspect(spider)},")
    Logger.metadata(spider: spider)
    log_file_path = setup_file_logger(spider, state[:log2file])
    Logger.info("#{log_prefix} setup file_logger: #{log_file_path} finish.")

    Logger.info("#{log_prefix} setup starting with spider_module: #{spider_module}.")

    state =
      state
      |> Map.merge(%{
        status: Map.get(state, :status, :running),
        log_prefix: log_prefix,
        log_file_path: log_file_path
      })
      |> prepare_for_start()

    Logger.info("#{log_prefix} setup prepare_for_start finish.")

    state =
      with true <- function_exported?(spider_module, :init, 1),
           state when is_map(state) <- spider_module.init(state) do
        Logger.info("#{log_prefix} setup init finish.")
        state
      else
        false ->
          state

        bad_state ->
          Logger.warning("#{log_prefix} setup init return bad state: #{inspect(bad_state)}.")
          state
      end

    Logger.info("#{log_prefix} setup done, status: #{state.status}.")
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  def handle_call(
        :suspend,
        _from,
        %{status: :running, log_prefix: log_prefix, stats_task_pid: stats_task_pid} = state
      ) do
    Stats.Task.suspend(stats_task_pid)
    true = call_components(state, :suspend) |> Enum.all?(&match?(:ok, &1))
    Logger.info("#{log_prefix} status turn to suspend.")
    {:reply, :ok, %{state | status: :suspended}}
  end

  def handle_call(:suspend, _from, state), do: {:reply, :ok, state}

  def handle_call({:suspend_component, component}, _from, state) do
    result =
      if get_component_pid(component, state) do
        Producer.call_producer(state.spider, component, :suspend)
      else
        :component_not_start
      end

    {:reply, result, state}
  end

  def handle_call(
        :continue,
        _from,
        %{status: :suspended, log_prefix: log_prefix, stats_task_pid: stats_task_pid} = state
      ) do
    Stats.Task.continue(stats_task_pid)
    true = call_components(state, :continue) |> Enum.all?(&match?(:ok, &1))
    Logger.info("#{log_prefix} status turn to running.")
    {:reply, :ok, %{state | status: :running}}
  end

  def handle_call(:continue, _from, state), do: {:reply, :ok, state}

  def handle_call({:continue_component, component}, _from, state) do
    result =
      if get_component_pid(component, state) do
        Producer.call_producer(state.spider, component, :continue)
      else
        :component_not_start
      end

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
      @ets_filename_key_mapping,
      fn {file_suffix, key} ->
        file_name = "#{file_name}_#{file_suffix}.ets"
        Logger.notice("starting dump2file: #{file_name} ...")
        result = Map.fetch!(state, key) |> Utils.dump_ets2file(file_name)
        Logger.notice("dump2file: #{file_name} finished, result: #{inspect(result)}.")
      end
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
    Logger.warning("unsupported call msg: #{msg}.")
    {:reply, :upsupported, state}
  end

  @impl true
  def handle_info({:EXIT, supervisor_pid, reason}, %{supervisor_pid: supervisor_pid} = state) do
    {:stop, reason, state}
  end

  @impl true
  def terminate(reason, %{supervisor_pid: supervisor_pid, log_prefix: log_prefix} = state) do
    # wait all broadway stopped then go continue
    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, reason_to_signal(reason))

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end

    level = if reason in [:normal, :shutdown], do: :info, else: :error
    Logger.log(level, "#{log_prefix} terminate by reason: #{inspect(reason)}.")

    prepare_for_stop(state, level)
    Logger.log(level, "#{log_prefix} prepare_for_stop finish.")

    Stats.detach_spider_stats()
    remove_file_logger(state)
    Logger.log(level, "#{log_prefix} Engine stopped.")
    :ok
  end

  defp prepare_for_stop(
         %{
           spider_module: spider_module,
           log_prefix: log_prefix,
           downloader_options: downloader_options,
           spider_options: spider_options,
           item_processor_options: item_processor_options
         } = state,
         level
       ) do
    # prepare_for_stop_component
    @components
    |> Enum.zip([downloader_options, spider_options, item_processor_options])
    |> Enum.each(fn {component, options} ->
      prepare_for_stop_component(component, options, spider_module, log_prefix)
    end)

    Logger.log(level, "#{log_prefix} components prepare_for_stop finish.")

    Requester.prepare_for_stop(downloader_options)
    Logger.log(level, "#{log_prefix} Requester prepare_for_stop finish.")

    Storage.prepare_for_stop(item_processor_options)
    Logger.log(level, "#{log_prefix} Storage prepare_for_stop finish.")

    # prepare_for_stop
    if function_exported?(spider_module, :prepare_for_stop, 1) do
      spider_module.prepare_for_stop(state)
    end
  end

  defp reason_to_signal(:killed), do: :kill
  defp reason_to_signal(other), do: other

  defp prepare_for_start(%{spider_module: spider_module} = state) do
    if function_exported?(spider_module, :prepare_for_start, 2) do
      state =
        spider_module.prepare_for_start(:pre, state)
        |> setup_ets_tables()
        |> setup_stats()
        |> setup_components()

      spider_module.prepare_for_start(:post, state)
    else
      state
      |> setup_ets_tables()
      |> setup_stats()
      |> setup_components()
    end
  end

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
      |> Producer.prepare_for_start_producer()

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

  defp call_components(state, msg) do
    [
      downloader: state.downloader_pid,
      spider: state.spider_pid,
      item_processor: state.item_processor_pid
    ]
    |> Enum.reject(&(&1 |> elem(1) |> is_nil()))
    |> Enum.map(fn {component, _pid} ->
      Producer.call_producer(state.spider, component, msg)
    end)
  end

  defp setup_ets_tables(%{spider: spider, log_prefix: log_prefix} = state) do
    state = do_setup_ets_tables(state)

    :persistent_term.put(spider, %{
      stats_tid: state.stats_tid,
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
        @ets_key_filename_mapping,
        fn {key, file_suffix} ->
          file_name = "#{file_name}_#{file_suffix}.ets"
          Logger.info("#{log_prefix} loading ets from file: #{file_name} ...")
          tid = Utils.setup_ets_from_file!(file_name)
          Logger.info("#{log_prefix} loading ets from file: #{file_name} finished.")
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
      stats_tid: :ets.new(:stats, producer_ets_options),
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

    :ets.insert(ets_tables.stats_tid, [
      # {Component, Total, Success, Fail, Duration}
      {:downloader, 0, 0, 0, 0},
      {:spider, 0, 0, 0, 0},
      {:item_processor, 0, 0, 0, 0}
    ])

    Map.merge(state, ets_tables)
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

    {components, children} =
      [
        {:downloader_pid, {Downloader, state.downloader_options}},
        {:spider_pid, {Spider, state.spider_options}},
        {:item_processor_pid, {ItemProcessor, state.item_processor_options}}
      ]
      |> Enum.filter(&match?({_, {_, options}} when is_list(options), &1))
      |> Enum.unzip()

    Supervisor.start_link(children, strategy: :one_for_one, max_restarts: 0, name: name)
    |> case do
      {:ok, supervisor_pid} ->
        Logger.info("#{state.log_prefix} setup components finish.")

        component_pids =
          Supervisor.which_children(supervisor_pid) |> Enum.reduce([], &[elem(&1, 1) | &2])

        [:supervisor_pid | components]
        |> Enum.zip([supervisor_pid | component_pids])
        |> Enum.into(state)

      error ->
        raise "start name: #{name} error: #{inspect(error)}"
    end
  end

  defp get_component_pid(:downloader, %{downloader_pid: pid}), do: pid
  defp get_component_pid(:spider, %{spider_pid: pid}), do: pid
  defp get_component_pid(:item_processor, %{item_processor_pid: pid}), do: pid

  defp setup_file_logger(_spider, false), do: :skiped

  defp setup_file_logger(spider, nil) do
    if Code.ensure_loaded?(LoggerFileBackend) do
      setup_file_logger(spider, true)
    else
      :skiped
    end
  end

  defp setup_file_logger(spider, true) do
    timestamp = System.system_time(:second)
    log_file_path = Path.join([System.tmp_dir(), inspect(spider), "#{timestamp}.log"])
    setup_file_logger(spider, log_file_path)
  end

  defp setup_file_logger(spider, log_file_path) do
    unless Code.ensure_loaded?(LoggerFileBackend) do
      raise "Please add :logger_file_backend lib to your deps."
    end

    backend = {LoggerFileBackend, spider}
    Logger.add_backend(backend)

    Logger.configure_backend(backend,
      path: log_file_path,
      level: :debug,
      metadata_filter: [spider: spider]
    )

    log_file_path
  end

  defp remove_file_logger(%{log_file_path: :skiped}), do: :skiped

  defp remove_file_logger(%{spider: spider}) do
    Logger.remove_backend({LoggerFileBackend, spider})
  end

  defp setup_stats(%{stats_tid: tid, spider: spider, status: status} = state) do
    Stats.attach_spider_stats(spider, tid)

    pid =
      if state.print_stats do
        {:ok, stats_task_pid} =
          Stats.Task.start_link(%{status: status, tid: tid, refresh_interval: 1000})

        stats_task_pid
      else
        nil
      end

    Map.put(state, :stats_task_pid, pid)
  end
end
