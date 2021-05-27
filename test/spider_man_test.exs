defmodule SpiderMan.SpiderManTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Utils
  # doctest SpiderMan

  setup_all do
    spider = SpiderManTest
    SpiderMan.start(spider)

    on_exit(fn ->
      SpiderMan.stop(spider)
    end)

    [spider: spider]
  end

  setup context do
    spider = context.spider

    if context[:spider_stopped] do
      SpiderMan.stop(spider)

      on_exit(fn ->
        {:ok, _} = SpiderMan.start(spider)
      end)
    end

    :ok
  end

  test "spider module", %{spider: spider} do
    assert function_exported?(spider, :handle_response, 2)
  end

  @tag spider_stopped: true
  test "start/stop Spider", %{spider: spider} do
    {:ok, pid} = SpiderMan.start(spider)
    assert is_pid(pid)
    assert Process.alive?(pid)
    assert {:error, {:already_started, _}} = SpiderMan.start(spider)

    assert :ok = SpiderMan.stop(spider)
    assert Process.alive?(pid) == false
    assert {:error, :not_found} = SpiderMan.stop(spider)
  end

  test "stats", %{spider: spider} do
    assert [
             status: :running,
             common_pipeline_tid: [memory: _, size: 0],
             downloader_tid: [memory: _, size: 0],
             failed_tid: [memory: _, size: 0],
             item_processor_tid: [memory: _, size: 0],
             spider_tid: [memory: _, size: 0]
           ] = SpiderMan.stats(spider)
  end

  test "list_spiders", %{spider: spider} do
    spiders = SpiderMan.list_spiders()
    assert is_list(spiders)
    assert spider in spiders
  end

  test "insert requests for spider", %{spider: spider} do
    requests = Utils.build_requests(1..10)
    :ok = SpiderMan.suspend(spider)
    assert SpiderMan.insert_requests(spider, requests)
    %{downloader_tid: tid} = SpiderMan.get_state(spider)
    assert 10 = :ets.info(tid, :size)
    :ok = SpiderMan.continue(spider)
  end

  test "suspend/continue Spider", %{spider: spider} do
    assert :ok = SpiderMan.suspend(spider)
    assert :suspended = SpiderMan.status(spider)
    assert :ok = SpiderMan.continue(spider)
    assert :running = SpiderMan.status(spider)
  end

  @tag spider_stopped: true
  test "run_until", %{spider: spider} do
    assert time = SpiderMan.run_until_zero(spider)
    assert System.convert_time_unit(time, :millisecond, :second) >= 3
    assert Process.whereis(spider) |> is_nil()
  end
end
