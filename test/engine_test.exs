defmodule SpiderMan.EngineTest do
  use ExUnit.Case, async: true
  alias SpiderMan.{Engine, CommonSpider, Request, Response, Item, Utils}

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
    # components
    assert %{supervisor_pid: _, downloader_pid: _, spider_pid: _, item_processor_pid: _} = state
    # ets tables
    assert %{
             downloader_tid: _,
             spider_tid: _,
             item_processor_tid: _,
             common_pipeline_tid: _,
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
             :pipeline_tid,
             :pipelines,
             :context
           ])

    assert state.spider_tid == spider_options[:tid]
    assert state.item_processor_tid == spider_options[:next_tid]
    assert state.common_pipeline_tid == spider_options[:common_pipeline_tid]
    assert state.spider_pipeline_tid == spider_options[:pipeline_tid]
    assert [] = spider_options[:pipelines]
    assert is_map(spider_options[:context])
    assert Enum.empty?(spider_options[:context])
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
        assert :suspended = Utils.producer_status(pid)
        assert :ok = Engine.continue_component(spider, component)
        assert :running = Utils.producer_status(pid)
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
      &assert(:suspended = Utils.producer_status(&1))
    )

    :ok = SpiderMan.continue(spider)
    assert :running = SpiderMan.status(spider)

    Enum.each(
      [downloader_pid, spider_pid, item_processor_pid],
      fn pid -> assert :running = Utils.producer_status(pid) end
    )
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
        downloader_tid: downloader_tid,
        spider_tid: spider_tid,
        item_processor_tid: item_processor_tid,
        common_pipeline_tid: common_pipeline_tid,
        downloader_pipeline_tid: downloader_pipeline_tid,
        spider_pipeline_tid: spider_pipeline_tid,
        item_processor_pipeline_tid: item_processor_pipeline_tid
      } = SpiderMan.get_state(spider)

      :ets.insert(downloader_tid, {1, %Request{key: 1, url: 1}})
      :ets.insert(spider_tid, {1, %Response{key: 1, env: %Tesla.Env{url: 1}}})
      :ets.insert(item_processor_tid, {1, %Item{key: 1, value: 1}})
      :ets.insert(common_pipeline_tid, {1, :common_pipeline_tid})
      :ets.insert(downloader_pipeline_tid, {1, :downloader_pipeline_tid})
      :ets.insert(spider_pipeline_tid, {1, :spider_pipeline_tid})
      :ets.insert(item_processor_pipeline_tid, {1, :item_processor_pipeline_tid})

      assert :ok = Engine.dump2file_force(spider, file_name)

      suffix_of_tables = [
        "downloader",
        "spider",
        "item_processor",
        "common_pipeline",
        "downloader_pipeline",
        "spider_pipeline",
        "item_processor_pipeline"
      ]

      assert Enum.all?(suffix_of_tables, &File.exists?("#{file_name}_#{&1}.ets"))
    end

    test "setup_ets_tables - load_from_file", %{spider: spider, file_name: file_name} do
      :ok = SpiderMan.stop(spider)
      assert {:ok, _} = SpiderMan.start(spider, load_from_file: file_name)
      Process.sleep(500)
      check_ets_tables(spider, 0, 1)
    end

    test "setup_ets_tables - suspended - load_from_file", %{spider: spider, file_name: file_name} do
      :ok = SpiderMan.stop(spider)
      assert {:ok, _} = SpiderMan.start(spider, load_from_file: file_name, status: :suspended)
      check_ets_tables(spider, 1, 1)

      assert %{
               status: :suspended,
               downloader_tid: downloader_tid,
               spider_tid: spider_tid,
               item_processor_tid: item_processor_tid,
               common_pipeline_tid: common_pipeline_tid,
               downloader_pipeline_tid: downloader_pipeline_tid,
               spider_pipeline_tid: spider_pipeline_tid,
               item_processor_pipeline_tid: item_processor_pipeline_tid
             } = SpiderMan.get_state(spider)

      assert %Request{key: 1, url: 1} = :ets.lookup_element(downloader_tid, 1, 2)
      assert %Response{key: 1, env: %Tesla.Env{url: 1}} = :ets.lookup_element(spider_tid, 1, 2)
      assert %Item{key: 1, value: 1} = :ets.lookup_element(item_processor_tid, 1, 2)
      assert :common_pipeline_tid = :ets.lookup_element(common_pipeline_tid, 1, 2)
      assert :downloader_pipeline_tid = :ets.lookup_element(downloader_pipeline_tid, 1, 2)
      assert :spider_pipeline_tid = :ets.lookup_element(spider_pipeline_tid, 1, 2)
      assert :item_processor_pipeline_tid = :ets.lookup_element(item_processor_pipeline_tid, 1, 2)
    end
  end

  defp check_ets_tables(spider, producer_ets_size \\ 0, pipeline_ets_size \\ 0) do
    state = SpiderMan.get_state(spider)

    %{
      downloader_tid: downloader_tid,
      spider_tid: spider_tid,
      item_processor_tid: item_processor_tid,
      common_pipeline_tid: common_pipeline_tid,
      downloader_pipeline_tid: downloader_pipeline_tid,
      spider_pipeline_tid: spider_pipeline_tid,
      item_processor_pipeline_tid: item_processor_pipeline_tid
    } = state

    tables = [
      downloader_tid,
      spider_tid,
      item_processor_tid,
      common_pipeline_tid,
      downloader_pipeline_tid,
      spider_pipeline_tid,
      item_processor_pipeline_tid
    ]

    count = Enum.count(tables)
    assert ^count = tables |> Enum.uniq() |> Enum.count()
    assert false == Enum.any?(tables, &is_nil/1)

    assert Enum.all?(
             [downloader_tid, spider_tid, item_processor_tid],
             &match?(^producer_ets_size, :ets.info(&1, :size))
           )

    assert Enum.all?(
             [
               common_pipeline_tid,
               downloader_pipeline_tid,
               spider_pipeline_tid,
               item_processor_pipeline_tid
             ],
             &match?(^pipeline_ets_size, :ets.info(&1, :size))
           )

    assert ^common_pipeline_tid = :persistent_term.get({spider, :common_pipeline_tid})
    assert ^downloader_tid = :persistent_term.get({spider, :downloader_tid})
    assert ^spider_tid = :persistent_term.get({spider, :spider_tid})
    assert ^item_processor_tid = :persistent_term.get({spider, :item_processor_tid})
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

  defp empty_handle_response_fun(), do: fn _response, _context -> %{} end
end
