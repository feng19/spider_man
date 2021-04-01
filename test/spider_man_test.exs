defmodule SpiderManTest do
  use ExUnit.Case
  doctest SpiderMan

  test "start Spider" do
    spider = Spider1

    {:ok, pid} =
      SpiderMan.start(spider, parent: self(), spider_options: [context: %{parent: self()}])

    assert_receive :started

    SpiderMan.stop(spider)
    assert Process.alive?(pid) == false
    assert_receive :stoped
  end
end
