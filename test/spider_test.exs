defmodule SpiderMan.SpiderTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias SpiderMan.{Engine, CommonSpider, Requester.JustReturn, Pipeline, Storage, Utils, Producer}

  setup_all do
    spider = SpiderTest

    on_exit(fn ->
      SpiderMan.stop(spider)
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
        log2file: false,
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

    assert %{
             status: ^status,
             downloader_pid: downloader_pid,
             spider_pid: spider_pid,
             item_processor_pid: item_processor_pid,
             common_pipeline_tid: common_pipeline_tid
           } = SpiderMan.get_state(spider)

    # whole flow: downloader -> requester -> spider -> item_processor -> storage

    # insert a request
    assert 0 = Pipeline.Counter.get(common_pipeline_tid)
    request = Utils.build_request(key)
    assert SpiderMan.insert_request(spider, request)

    # continue downloader
    assert :ok = Engine.continue_component(spider, :downloader)
    assert :running = Utils.producer_status(downloader_pid)
    Process.sleep(100)
    assert 1 = Pipeline.Counter.get(common_pipeline_tid)

    # continue spider
    assert :ok = Engine.continue_component(spider, :spider)
    assert :running = Utils.producer_status(spider_pid)
    Process.sleep(100)
    assert 2 = Pipeline.Counter.get(common_pipeline_tid)

    # continue item_processor and capture storage log
    assert capture_log([level: :info], fn ->
             assert :ok = Engine.continue_component(spider, :item_processor)
             assert :running = Utils.producer_status(item_processor_pid)
             Process.sleep(100)
             assert 3 = Pipeline.Counter.get(common_pipeline_tid)
           end) =~ ">> store item:"
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

    {:ok, _pid} = CommonSpider.start(spider, [handle_response: handle_response], settings)

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

    producer = {Producer.ETS, [retry_interval: retry_interval]}

    settings = [
      downloader_options: [
        producer: producer,
        rate_limiting: nil,
        pipelines: pipelines,
        processor: processor,
        requester: JustReturn
      ],
      spider_options: [
        producer: producer,
        pipelines: pipelines,
        processor: processor
      ],
      item_processor_options: [
        producer: producer,
        pipelines: pipelines,
        processor: processor,
        batchers: []
      ]
    ]

    {:ok, _pid} = CommonSpider.start(spider, [handle_response: handle_response], settings)

    assert %{
             downloader_pipeline_tid: downloader_pipeline_tid,
             spider_pipeline_tid: spider_pipeline_tid,
             item_processor_pipeline_tid: item_processor_pipeline_tid
           } = SpiderMan.get_state(spider)

    requests = 1..10000 |> Enum.map(&{&1, 1}) |> Utils.build_requests()
    assert SpiderMan.insert_requests(spider, requests)
    total = 500_000
    wait_until_count(downloader_pipeline_tid, total)
    wait_until_count(spider_pipeline_tid, total)
    wait_until_count(item_processor_pipeline_tid, total)

    SpiderMan.stop(spider)
  end

  defp wait_until_count(tid, count) do
    if Pipeline.Counter.get(tid) < count do
      Process.sleep(100)
      wait_until_count(tid, count)
    else
      :ok
    end
  end
end
