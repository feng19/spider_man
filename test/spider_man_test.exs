defmodule SpiderManTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Utils
  # doctest SpiderMan

  setup_all do
    spider = SpiderMan.Modules.setup_all()

    on_exit(fn ->
      SpiderMan.Modules.on_exit(spider)
    end)

    [spider: spider]
  end

  setup context do
    spider = context.spider

    if context[:spider_stopped] do
      SpiderMan.stop(spider)

      on_exit(fn ->
        {:ok, _} = SpiderMan.ensure_started(spider)
      end)
    end

    :ok
  end

  test "spider module", %{spider: spider} do
    assert function_exported?(spider, :start_link, 1)
    assert function_exported?(spider, :init, 1)
    assert function_exported?(spider, :handle_response, 2)
  end

  @tag spider_stopped: true
  test "start/stop Spider", %{spider: spider} do
    {:ok, pid} = SpiderMan.start(spider)
    SpiderMan.wait_until(spider)
    assert is_pid(pid)
    assert Process.alive?(pid)
    assert {:error, {:already_started, _}} = SpiderMan.start(spider)

    assert :ok = SpiderMan.stop(spider)
    assert Process.alive?(pid) == false
    assert {:error, :not_found} = SpiderMan.stop(spider)
  end

  test "stats", %{spider: spider} do
    SpiderMan.wait_until(spider)

    assert [
             status: :running,
             downloader: [memory: _, size: 0],
             spider: [memory: _, size: 0],
             item_processor: [memory: _, size: 0]
           ] = SpiderMan.stats(spider)
  end

  test "list_spiders", %{spider: spider} do
    spiders = SpiderMan.list_spiders()
    assert is_list(spiders)
    assert spider in spiders
  end

  test "insert requests for spider", %{spider: spider} do
    requests = Utils.build_requests(1..10)
    assert SpiderMan.insert_requests(spider, requests)
  end

  test "suspend/continue Spider", %{spider: spider} do
    assert :ok = SpiderMan.suspend(spider)
    assert :suspended = SpiderMan.status(spider)
    assert :ok = SpiderMan.continue(spider)
    assert :running = SpiderMan.status(spider)
  end

  test "one_for_all", %{spider: spider} do
    assert :ok = SpiderMan.suspend(spider)
    Process.sleep(100)

    %{
      component_sup: component_sup,
      downloader_pid: downloader_pid,
      spider_pid: spider_pid,
      item_processor_pid: item_processor_pid
    } = SpiderMan.get_state(spider)

    spider_sup_pid = Process.whereis(spider)
    engine_pid = get_engine_pid(spider)
    Process.exit(engine_pid, :kill)
    Process.sleep(100)
    assert not Process.alive?(engine_pid)
    assert not Process.alive?(component_sup)
    assert not Process.alive?(downloader_pid)
    assert not Process.alive?(spider_pid)
    assert not Process.alive?(item_processor_pid)
    assert Process.alive?(spider_sup_pid)

    SpiderMan.wait_until(spider)
    engine_pid = get_engine_pid(spider)
    assert Process.alive?(engine_pid)
    assert :running = SpiderMan.status(spider)

    %{
      component_sup: component_sup,
      downloader_pid: downloader_pid,
      spider_pid: spider_pid,
      item_processor_pid: item_processor_pid
    } = SpiderMan.get_state(spider)

    Process.exit(downloader_pid, :kill)
    Process.sleep(100)
    assert not Process.alive?(downloader_pid)
    assert not Process.alive?(spider_pid)
    assert not Process.alive?(item_processor_pid)
    assert not Process.alive?(component_sup)
    assert not Process.alive?(engine_pid)
    assert Process.alive?(spider_sup_pid)

    SpiderMan.wait_until(spider)
    engine_pid = get_engine_pid(spider)
    assert Process.alive?(engine_pid)
    assert :running = SpiderMan.status(spider)
  end

  defp get_engine_pid(spider) do
    spider
    |> SpiderMan.Engine.process_name()
    |> Process.whereis()
  end
end
