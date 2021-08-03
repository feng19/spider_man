defmodule SpiderMan.EngineTest do
  use ExUnit.Case, async: true

  alias SpiderMan.{
    CommonSpider,
    Engine,
    Item,
    Pipeline,
    Producer,
    Request,
    Requester.JustReturn,
    Response,
    Utils
  }

  setup_all do
    spider = EngineTest
    SpiderMan.start(spider)

    on_exit(fn ->
      SpiderMan.stop(spider)
    end)

    [spider: spider]
  end

  test "check state", %{spider: spider} do
    state = SpiderMan.get_state(spider)
    assert %{spider: ^spider, spider_module: ^spider, status: :running} = state
    # options
    assert %{downloader_options: _, spider_options: _, item_processor_options: _} = state
    # pids
    assert %{
             supervisor_pid: _,
             downloader_pid: _,
             spider_pid: _,
             item_processor_pid: _,
             stats_task_pid: _
           } = state

    # ets tables
    assert %{
             # common ets
             stats_tid: _,
             failed_tid: _,
             common_pipeline_tid: _,
             # producer ets
             downloader_tid: _,
             spider_tid: _,
             item_processor_tid: _,
             # pipeline ets
             downloader_pipeline_tid: _,
             spider_pipeline_tid: _,
             item_processor_pipeline_tid: _
           } = state
  end

  test "check spider_module", %{spider: spider} do
    on_exit(fn ->
      SpiderMan.stop(spider)
      SpiderMan.start(spider)
    end)

    SpiderMan.stop(spider)

    CommonSpider.start(spider, [
      {:handle_response, empty_handle_response_fun()}
    ])

    state = SpiderMan.get_state(spider)
    assert %{spider: ^spider, spider_module: CommonSpider, callbacks: _, status: :running} = state
  end

  test "check downloader_options", %{spider: spider} do
    state = SpiderMan.get_state(spider)
    downloader_options = state.downloader_options
    assert Keyword.keyword?(downloader_options)

    assert has_keys?(downloader_options, [
             :spider,
             :spider_module,
             :tid,
             :next_tid,
             :common_pipeline_tid,
             :failed_tid,
             :pipeline_tid,
             :pipelines,
             :context
           ])

    assert state.downloader_tid == downloader_options[:tid]
    assert state.spider_tid == downloader_options[:next_tid]
    assert state.common_pipeline_tid == downloader_options[:common_pipeline_tid]
    assert state.downloader_pipeline_tid == downloader_options[:pipeline_tid]
    assert [] = downloader_options[:pipelines]
    assert %{requester: _} = downloader_options[:context]
  end

  test "check spider_options", %{spider: spider} do
    state = SpiderMan.get_state(spider)
    spider_options = state.spider_options
    assert Keyword.keyword?(spider_options)

    assert has_keys?(spider_options, [
             :spider,
             :spider_module,
             :tid,
             :next_tid,
             :common_pipeline_tid,
             :failed_tid,
             :pipeline_tid,
             :pipelines,
             :context
           ])

    assert state.spider_tid == spider_options[:tid]
    assert state.item_processor_tid == spider_options[:next_tid]
    assert state.common_pipeline_tid == spider_options[:common_pipeline_tid]
    assert state.spider_pipeline_tid == spider_options[:pipeline_tid]
    assert [] = spider_options[:pipelines]
    assert %{pipelines: [], post_pipelines: []} = spider_options[:context]
  end

  test "check item_processor_options", %{spider: spider} do
    state = SpiderMan.get_state(spider)
    item_processor_options = state.item_processor_options
    assert Keyword.keyword?(item_processor_options)

    assert has_keys?(item_processor_options, [
             :spider,
             :spider_module,
             :tid,
             :common_pipeline_tid,
             :failed_tid,
             :pipeline_tid,
             :pipelines,
             :context
           ])

    assert state.item_processor_tid == item_processor_options[:tid]
    assert state.common_pipeline_tid == item_processor_options[:common_pipeline_tid]
    assert state.item_processor_pipeline_tid == item_processor_options[:pipeline_tid]
    assert [] = item_processor_options[:pipelines]
    assert %{storage: _, storage_context: _} = item_processor_options[:context]
  end

  describe "prepare_for_* callbacks" do
    setup %{spider: spider} do
      SpiderMan.stop(spider)

      on_exit(fn ->
        SpiderMan.stop(spider)
        {:ok, _} = SpiderMan.start(spider)
      end)
    end

    defp setup_callback(spider, key, 1) do
      parent = self()

      fun = fn state ->
        send(parent, {spider, key})
        state
      end

      setup_callback(spider, key, fun)
    end

    defp setup_callback(spider, key, 2) do
      parent = self()

      fun = fn stage_or_component, options ->
        send(parent, {spider, key, stage_or_component})
        options
      end

      setup_callback(spider, key, fun)
    end

    defp setup_callback(spider, key, fun) do
      CommonSpider.start(spider, [
        {:handle_response, empty_handle_response_fun()},
        {key, fun}
      ])
    end

    test "prepare_for_start", %{spider: spider} do
      key = :prepare_for_start
      setup_callback(spider, key, 2)
      assert_receive {^spider, ^key, :pre}
      assert_receive {^spider, ^key, :post}
    end

    test "prepare_for_start_component", %{spider: spider} do
      key = :prepare_for_start_component
      setup_callback(spider, key, 2)
      assert_receive {^spider, ^key, :downloader}, 200
      assert_receive {^spider, ^key, :spider}
      assert_receive {^spider, ^key, :item_processor}
    end

    test "prepare_for_stop", %{spider: spider} do
      key = :prepare_for_stop
      setup_callback(spider, key, 1)
      SpiderMan.stop(spider)
      assert_receive {^spider, ^key}
    end

    test "prepare_for_stop_component", %{spider: spider} do
      key = :prepare_for_stop_component
      setup_callback(spider, key, 2)
      SpiderMan.stop(spider)
      assert_receive {^spider, ^key, :downloader}, 200
      assert_receive {^spider, ^key, :spider}
      assert_receive {^spider, ^key, :item_processor}
    end
  end

  test "suspend/continue_component", %{spider: spider} do
    %{
      downloader_pid: downloader_pid,
      spider_pid: spider_pid,
      item_processor_pid: item_processor_pid
    } = SpiderMan.get_state(spider)

    Enum.each(
      [downloader: downloader_pid, spider: spider_pid, item_processor: item_processor_pid],
      fn {component, pid} ->
        assert :ok = Engine.suspend_component(spider, component)
        assert :suspended = Producer.producer_status(pid)
        assert :ok = Engine.continue_component(spider, component)
        assert :running = Producer.producer_status(pid)
      end
    )
  end

  test "continue_status", %{spider: spider} do
    :ok = SpiderMan.stop(spider)
    status = :suspended
    assert {:ok, _} = SpiderMan.start(spider, status: status)

    assert %{
             status: ^status,
             downloader_pid: downloader_pid,
             spider_pid: spider_pid,
             item_processor_pid: item_processor_pid
           } = SpiderMan.get_state(spider)

    Enum.each(
      [downloader_pid, spider_pid, item_processor_pid],
      &assert(:suspended = Producer.producer_status(&1))
    )

    :ok = SpiderMan.continue(spider)
    assert :running = SpiderMan.status(spider)

    Enum.each(
      [downloader_pid, spider_pid, item_processor_pid],
      fn pid -> assert :running = Producer.producer_status(pid) end
    )
  end

  test "retry_failed" do
    {:ok, agent} = Agent.start_link(fn -> {:error, :anything} end)
    handle_response = fn _response, _context -> Agent.get(agent, & &1) end
    spider = :retry_failed

    assert {:ok, _pid} =
             CommonSpider.start(
               spider,
               [handle_response: handle_response],
               status: :suspended,
               downloader_options: [pipelines: [Pipeline.Counter], requester: JustReturn],
               spider_options: [pipelines: [Pipeline.Counter]],
               item_processor_options: [storage: false]
             )

    assert %{
             failed_tid: failed_tid,
             downloader_pipeline_tid: downloader_pipeline_tid,
             spider_pipeline_tid: spider_pipeline_tid
           } = SpiderMan.get_state(spider)

    assert true = SpiderMan.insert_request(spider, Utils.build_request("1"))
    assert 0 = Pipeline.Counter.get(downloader_pipeline_tid)
    assert 0 = Pipeline.Counter.get(spider_pipeline_tid)
    :ok = SpiderMan.continue(spider)
    Process.sleep(300)

    assert 1 = Pipeline.Counter.get(downloader_pipeline_tid)
    assert 1 = Pipeline.Counter.get(spider_pipeline_tid)
    assert [{{:spider, "1"}, %Response{retries: -1}}] = :ets.tab2list(failed_tid)

    :ok = Agent.update(agent, fn _ -> %{} end)
    assert {:ok, 1} = SpiderMan.retry_failed(spider)
    assert [] = :ets.tab2list(failed_tid)
    Process.sleep(200)

    assert 1 = Pipeline.Counter.get(downloader_pipeline_tid)
    assert 2 = Pipeline.Counter.get(spider_pipeline_tid)
    assert :ok = SpiderMan.stop(spider)
  end

  test "setup_ets_tables - new", %{spider: spider} do
    check_ets_tables(spider)
  end

  describe "dump_and_load_ets_tables" do
    @describetag :tmp_dir

    setup %{spider: spider, tmp_dir: tmp_dir} do
      dir =
        tmp_dir
        |> Path.join("../test-dump_and_load_ets_tables")
        |> Path.expand()

      file_name = Path.join(dir, inspect(spider))
      [file_name: file_name]
    end

    test "dump2file", %{spider: spider, file_name: file_name} do
      assert :ok = SpiderMan.suspend(spider)
      assert :suspended = SpiderMan.status(spider)

      # insert some records to ets tables

      %{
        # common ets
        failed_tid: failed_tid,
        common_pipeline_tid: common_pipeline_tid,
        # producer ets
        downloader_tid: downloader_tid,
        spider_tid: spider_tid,
        item_processor_tid: item_processor_tid,
        # pipeline ets
        downloader_pipeline_tid: downloader_pipeline_tid,
        spider_pipeline_tid: spider_pipeline_tid,
        item_processor_pipeline_tid: item_processor_pipeline_tid
      } = SpiderMan.get_state(spider)

      :ets.insert(downloader_tid, {1, %Request{key: 1, url: 1}})
      :ets.insert(spider_tid, {1, %Response{key: 1, env: %Tesla.Env{url: 1}}})
      :ets.insert(item_processor_tid, {1, %Item{key: 1, value: 1}})
      :ets.insert(common_pipeline_tid, {1, :common_pipeline_tid})
      :ets.insert(failed_tid, {1, :failed_tid})
      :ets.insert(downloader_pipeline_tid, {1, :downloader_pipeline_tid})
      :ets.insert(spider_pipeline_tid, {1, :spider_pipeline_tid})
      :ets.insert(item_processor_pipeline_tid, {1, :item_processor_pipeline_tid})

      assert :ok = Engine.dump2file_force(spider, file_name)

      suffix_of_tables = [
        # common ets
        "stats",
        "failed",
        "common_pipeline",
        # producer ets
        "downloader",
        "spider",
        "item_processor",
        # pipeline ets
        "downloader_pipeline",
        "spider_pipeline",
        "item_processor_pipeline"
      ]

      assert Enum.all?(suffix_of_tables, &File.exists?("#{file_name}_#{&1}.ets"))
    end

    test "setup_ets_tables - setup_ets_from_file", %{spider: spider, file_name: file_name} do
      :ok = SpiderMan.stop(spider)
      assert {:ok, _} = SpiderMan.start(spider, ets_file: file_name)
      Process.sleep(500)
      check_ets_tables(spider, 0, 1)
    end

    test "setup_ets_tables - suspended - setup_ets_from_file", %{
      spider: spider,
      file_name: file_name
    } do
      :ok = SpiderMan.stop(spider)
      assert {:ok, _} = SpiderMan.start(spider, ets_file: file_name, status: :suspended)
      check_ets_tables(spider, 1, 1)

      assert %{
               status: :suspended,
               # common ets
               failed_tid: failed_tid,
               common_pipeline_tid: common_pipeline_tid,
               # producer ets
               downloader_tid: downloader_tid,
               spider_tid: spider_tid,
               item_processor_tid: item_processor_tid,
               # pipeline ets
               downloader_pipeline_tid: downloader_pipeline_tid,
               spider_pipeline_tid: spider_pipeline_tid,
               item_processor_pipeline_tid: item_processor_pipeline_tid
             } = SpiderMan.get_state(spider)

      assert %Request{key: 1, url: 1} = :ets.lookup_element(downloader_tid, 1, 2)
      assert %Response{key: 1, env: %Tesla.Env{url: 1}} = :ets.lookup_element(spider_tid, 1, 2)
      assert %Item{key: 1, value: 1} = :ets.lookup_element(item_processor_tid, 1, 2)
      assert :failed_tid = :ets.lookup_element(failed_tid, 1, 2)
      assert :common_pipeline_tid = :ets.lookup_element(common_pipeline_tid, 1, 2)
      assert :downloader_pipeline_tid = :ets.lookup_element(downloader_pipeline_tid, 1, 2)
      assert :spider_pipeline_tid = :ets.lookup_element(spider_pipeline_tid, 1, 2)
      assert :item_processor_pipeline_tid = :ets.lookup_element(item_processor_pipeline_tid, 1, 2)
    end
  end

  defp check_ets_tables(spider, producer_ets_size \\ 0, pipeline_ets_size \\ 0) do
    state = SpiderMan.get_state(spider)

    %{
      # common ets
      stats_tid: stats_tid,
      failed_tid: failed_tid,
      common_pipeline_tid: common_pipeline_tid,
      # producer ets
      downloader_tid: downloader_tid,
      spider_tid: spider_tid,
      item_processor_tid: item_processor_tid,
      # pipeline ets
      downloader_pipeline_tid: downloader_pipeline_tid,
      spider_pipeline_tid: spider_pipeline_tid,
      item_processor_pipeline_tid: item_processor_pipeline_tid
    } = state

    tables = [
      # common ets
      stats_tid,
      failed_tid,
      common_pipeline_tid,
      # producer ets
      downloader_tid,
      spider_tid,
      item_processor_tid,
      # pipeline ets
      downloader_pipeline_tid,
      spider_pipeline_tid,
      item_processor_pipeline_tid
    ]

    count = Enum.count(tables)
    assert ^count = tables |> Enum.uniq() |> Enum.count()
    assert false == Enum.any?(tables, &is_nil/1)

    assert [:downloader, :item_processor, :spider] =
             :ets.tab2list(stats_tid) |> Stream.map(&elem(&1, 0)) |> Enum.sort()

    assert Enum.all?(
             [downloader_tid, spider_tid, item_processor_tid],
             &match?(^producer_ets_size, :ets.info(&1, :size))
           )

    assert Enum.all?(
             [
               # common ets
               failed_tid,
               common_pipeline_tid,
               # pipeline ets
               downloader_pipeline_tid,
               spider_pipeline_tid,
               item_processor_pipeline_tid
             ],
             &match?(^pipeline_ets_size, :ets.info(&1, :size))
           )

    assert %{
             failed_tid: ^failed_tid,
             common_pipeline_tid: ^common_pipeline_tid,
             downloader_tid: ^downloader_tid,
             spider_tid: ^spider_tid,
             item_processor_tid: ^item_processor_tid
           } = :persistent_term.get(spider)
  end

  defp has_keys?(keyword, keys) do
    all_keys =
      keyword
      |> Keyword.keys()
      |> MapSet.new()

    keys
    |> MapSet.new()
    |> MapSet.subset?(all_keys)
  end

  defp empty_handle_response_fun, do: fn _response, _context -> %{} end
end
