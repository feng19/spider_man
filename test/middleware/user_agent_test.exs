defmodule SpiderMan.Middleware.UserAgentTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Middleware.UserAgent

  test "put headers" do
    assert {:ok, env} = UserAgent.call(%Tesla.Env{}, [], ["test"])
    assert "test" = Tesla.get_header(env, "user-agent")

    list = ["a", "b"]
    assert {:ok, env} = UserAgent.call(%Tesla.Env{}, [], list)
    assert Tesla.get_header(env, "user-agent") in list
  end
end
