defmodule SpiderMan.SpiderTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias SpiderMan.{Engine, CommonSpider, Requester.JustReturn, Pipeline, Storage, Utils}

  setup_all do
    spider = SpiderTest

    on_exit(fn ->
      SpiderMan.stop(SpiderTest)
    end)

    [spider: spider]
  end

  # merge_settings

  # check settings

  test "whole flow", %{spider: spider} do
    SpiderMan.stop(spider)
    key = "/"
    pipelines = [{Pipeline.Counter, :common}]
    status = :suspended

    handle_response = fn response, _context ->
      %{items: [Utils.build_item(response.key, "test")]}
    end

    {:ok, _pid} =
      CommonSpider.start(spider, [handle_response: handle_response],
        status: status,
        downloader_options: [pipelines: pipelines, requester: JustReturn],
        spider_options: [pipelines: pipelines],
        item_processor_options: [
          pipelines: pipelines,
          storage: Storage.Log,
          batchers: [
            default: [
              concurrency: 1,
              batch_size: 1,
              batch_timeout: 100
            ]
          ]
        ]
      )

    SpiderMan.wait_until(spider, status)

    assert %{
             status: ^status,
             downloader_pid: downloader_pid,
             spider_pid: spider_pid,
             item_processor_pid: item_processor_pid,
             common_pipeline_tid: common_pipeline_tid
           } = SpiderMan.get_state(spider)

    # downloader -> requester -> spider -> item_processor -> storage
    assert 0 = :ets.lookup_element(common_pipeline_tid, Pipeline.Counter, 2)
    request = Utils.build_request(key)
    assert SpiderMan.insert_requests(spider, [request])
    assert :ok = Engine.continue_component(spider, :downloader)
    assert :running = Utils.producer_status(downloader_pid)
    Process.sleep(100)
    assert 1 = :ets.lookup_element(common_pipeline_tid, Pipeline.Counter, 2)
    assert :ok = Engine.continue_component(spider, :spider)
    assert :running = Utils.producer_status(spider_pid)
    Process.sleep(100)
    assert 2 = :ets.lookup_element(common_pipeline_tid, Pipeline.Counter, 2)

    assert capture_log([level: :info], fn ->
             assert :ok = Engine.continue_component(spider, :item_processor)
             assert :running = Utils.producer_status(item_processor_pid)
             Process.sleep(100)
             assert 3 = :ets.lookup_element(common_pipeline_tid, Pipeline.Counter, 2)
           end) =~ "value: \"test\""
  end

  @tag :tmp_dir
  test "whole flow - save to json lines file", %{tmp_dir: tmp_dir, spider: spider} do
    SpiderMan.stop(spider)
    pipelines = []
    file_path = Path.join(tmp_dir, "data_#{System.system_time(:second)}.jsonl")

    handle_response = fn %{key: key}, _context ->
      %{items: [Utils.build_item(key, "test-#{key}")]}
    end

    settings = [
      downloader_options: [rate_limiting: nil, pipelines: pipelines, requester: JustReturn],
      spider_options: [pipelines: pipelines],
      item_processor_options: [
        pipelines: pipelines,
        storage: {Storage.JsonLines, file_path},
        batchers: [
          default: [
            concurrency: 1,
            batch_size: 1,
            batch_timeout: 100
          ]
        ]
      ]
    ]

    {:ok, _pid} =
      CommonSpider.ensure_started(spider, [handle_response: handle_response], settings)

    num_str_list = Enum.map(1..10, &to_string/1)
    requests = Utils.build_requests(num_str_list)
    assert SpiderMan.insert_requests(spider, requests)
    Process.sleep(1000)
    assert :ok = SpiderMan.stop(spider)
    Process.sleep(100)
    string = File.read!(file_path)
    list = String.split(string, "\n", trim: true)
    assert 10 = length(list)
    assert Enum.all?(list, &String.starts_with?(&1, "\"test-"))
    num_list = Enum.to_list(1..10)

    assert ^num_list =
             Enum.map(list, fn str ->
               str
               |> String.trim_leading("\"test-")
               |> String.trim_trailing("\"")
               |> String.to_integer()
             end)
             |> Enum.sort()
  end

  @tag :long_time
  test "high-concurrency", %{spider: spider} do
    SpiderMan.stop(spider)
    pipelines = [Pipeline.Counter]
    processor = [concurrency: 20, min_demand: 15, max_demand: 30]
    retry_interval = 50

    handle_response = fn
      %{key: {key, 50}}, _context ->
        %{items: [Utils.build_item({key, 1000}, 1000)]}

      %{key: {key, n}}, _context ->
        request = Utils.build_request({key, n + 1})
        item = Utils.build_item({key, n}, n)
        %{requests: [request], items: [item]}
    end

    settings = [
      downloader_options: [
        rate_limiting: nil,
        pipelines: pipelines,
        processor: processor,
        retry_interval: retry_interval,
        requester: JustReturn
      ],
      spider_options: [pipelines: pipelines, processor: processor, retry_interval: retry_interval],
      item_processor_options: [
        pipelines: pipelines,
        processor: processor,
        retry_interval: retry_interval,
        batchers: []
      ]
    ]

    {:ok, _pid} =
      CommonSpider.ensure_started(spider, [handle_response: handle_response], settings)

    assert %{
             downloader_pipeline_tid: downloader_pipeline_tid,
             spider_pipeline_tid: spider_pipeline_tid,
             item_processor_pipeline_tid: item_processor_pipeline_tid
           } = SpiderMan.get_state(spider)

    requests = 1..10000 |> Enum.map(&{&1, 1}) |> Utils.build_requests()
    assert SpiderMan.insert_requests(spider, requests)
    Process.sleep(5_500)
    SpiderMan.suspend(spider)
    Process.sleep(500)

    IO.inspect(
      downloader: :ets.lookup_element(downloader_pipeline_tid, Pipeline.Counter, 2),
      spider: :ets.lookup_element(spider_pipeline_tid, Pipeline.Counter, 2),
      item_processor: :ets.lookup_element(item_processor_pipeline_tid, Pipeline.Counter, 2)
    )

    SpiderMan.stop(spider)
  end
end
