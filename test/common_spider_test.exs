defmodule SpiderMan.CommonSpiderTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias SpiderMan.{CommonSpider, Item}

  setup_all do
    on_exit(fn ->
      SpiderMan.stop(CommonSpiderTest)
    end)
  end

  test "check callbacks" do
    spider = CommonSpiderTest
    wrong_fun = fn -> nil end
    handle_response = fn _response, _context -> %{} end
    prepare_for_start = fn _stage, state -> state end
    prepare_for_stop = fn _state -> :ok end
    prepare_for_start_component = fn _component, options -> options end
    prepare_for_stop_component = fn _component, _options -> :ok end
    module_str = inspect(CommonSpider)

    # missing callbacks
    error_msg = "Please defined :callbacks option when use #{module_str}."
    assert {:error, ^error_msg} = CommonSpider.start_link([])

    # wrong callbacks
    error_msg = "Bad type of :callbacks option for #{module_str}, please use Keyword."
    assert {:error, ^error_msg} = CommonSpider.start_link(callbacks: :wrong_type)

    # missing handle_response
    error_msg = "Must defined :handle_response for :callbacks option when use #{module_str}."
    assert {:error, ^error_msg} = CommonSpider.start_link(callbacks: [])

    # wrong callback - handle_response
    error_msg =
      "Wrong type of handle_response: #{inspect(:wrong_type)} defined in :callbacks option when use #{
        inspect(CommonSpider)
      }, please use fun/2 for this option."

    assert {:error, ^error_msg} =
             CommonSpider.start_link(callbacks: [handle_response: :wrong_type])

    error_msg =
      "Wrong type of handle_response: #{inspect(wrong_fun)} defined in :callbacks option when use #{
        inspect(CommonSpider)
      }, please use fun/2 for this option."

    assert {:error, ^error_msg} = CommonSpider.start_link(callbacks: [handle_response: wrong_fun])

    # wrong callback - prepare_for_start
    test_wrong_callback(:prepare_for_start, 2, handle_response, wrong_fun)

    # wrong callback - prepare_for_stop
    test_wrong_callback(:prepare_for_stop, 1, handle_response, wrong_fun)

    # wrong callback - prepare_for_start_component
    test_wrong_callback(:prepare_for_start_component, 2, handle_response, wrong_fun)

    # wrong callback - prepare_for_stop_component
    test_wrong_callback(:prepare_for_stop_component, 2, handle_response, wrong_fun)

    assert {:ok, _pid} =
             CommonSpider.start(spider,
               handle_response: handle_response,
               prepare_for_start: prepare_for_start,
               prepare_for_stop: prepare_for_stop,
               prepare_for_start_component: prepare_for_start_component,
               prepare_for_stop_component: prepare_for_stop_component
             )

    SpiderMan.stop(spider)
  end

  defp test_wrong_callback(key, arity, handle_response, wrong_fun) do
    spider = CommonSpiderTest

    assert capture_log([level: :warn], fn ->
             {:ok, _pid} =
               CommonSpider.start(spider, [
                 {:handle_response, handle_response},
                 {key, :wrong_type}
               ])

             SpiderMan.stop(spider)
           end) =~
             "Wrong type of #{to_string(key)}: :wrong_type defined in :callbacks option when use #{
               inspect(CommonSpider)
             }, please use fun/#{arity} for this option."

    assert capture_log([level: :warn], fn ->
             {:ok, _pid} =
               CommonSpider.start(spider, [
                 {:handle_response, handle_response},
                 {key, wrong_fun}
               ])

             SpiderMan.stop(spider)
           end) =~
             "Wrong type of #{to_string(key)}: #{inspect(wrong_fun)} defined in :callbacks option when use #{
               inspect(CommonSpider)
             }, please use fun/#{arity} for this option."
  end

  test "setup_callbacks" do
    spider = CommonSpiderTest
    handle_response = fn _response, _context -> %{} end
    prepare_for_start_component = fn _component, options -> options end
    prepare_for_stop_component = fn _component, _options -> :ok end

    assert {:ok, _pid} = CommonSpider.start(spider, handle_response: handle_response)
    SpiderMan.wait_until(spider)
    state = SpiderMan.get_state(spider)
    assert false == Keyword.has_key?(state.downloader_options, :prepare_for_start)
    assert false == Keyword.has_key?(state.downloader_options, :prepare_for_stop)
    assert %{callback: ^handle_response} = Keyword.get(state.spider_options, :context)
    assert false == Keyword.has_key?(state.spider_options, :prepare_for_start)
    assert false == Keyword.has_key?(state.spider_options, :prepare_for_stop)
    assert false == Keyword.has_key?(state.item_processor_options, :prepare_for_start)
    assert false == Keyword.has_key?(state.item_processor_options, :prepare_for_stop)

    SpiderMan.stop(spider)

    assert {:ok, _pid} =
             CommonSpider.start(spider,
               handle_response: handle_response,
               prepare_for_start_component: prepare_for_start_component,
               prepare_for_stop_component: prepare_for_stop_component
             )

    SpiderMan.wait_until(spider)
    state = SpiderMan.get_state(spider)
    assert Keyword.has_key?(state.downloader_options, :prepare_for_start)
    assert Keyword.has_key?(state.downloader_options, :prepare_for_stop)
    assert %{callback: ^handle_response} = Keyword.get(state.spider_options, :context)
    assert Keyword.has_key?(state.spider_options, :prepare_for_start)
    assert Keyword.has_key?(state.spider_options, :prepare_for_stop)
    assert Keyword.has_key?(state.item_processor_options, :prepare_for_start)
    assert Keyword.has_key?(state.item_processor_options, :prepare_for_stop)
  end

  test "handle_response callback", %{test: test} do
    callback = fn response, context ->
      item = %Item{key: response.url, value: context}
      %{items: [item]}
    end

    context = %{callback: callback}

    assert %{items: [%Item{key: ^test, value: ^context}]} =
             CommonSpider.handle_response(%Tesla.Env{url: test}, context)
  end
end
