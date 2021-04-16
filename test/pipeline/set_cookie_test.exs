defmodule SpiderMan.Pipeline.SetCookieTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Pipeline.SetCookie

  test "prepare_for_start" do
    tid = new_ets_table()
    options = [common_pipeline_tid: tid]
    # empty cookies
    assert {%{tid: ^tid, agent: agent}, ^options} = SetCookie.prepare_for_start(nil, options)
    assert [] = Agent.get(agent, & &1)

    assert [cookies: [], cookies_agent: ^agent, cookies_str: ""] =
             :ets.tab2list(tid) |> Enum.sort()

    # only set cookies to ets
    old_agent = agent
    cookies = ["1", "2", "3"]
    assert :ets.insert(tid, cookies: cookies)
    :ets.delete(tid, :cookies_agent)
    assert {%{tid: ^tid, agent: agent}, ^options} = SetCookie.prepare_for_start(nil, options)
    assert old_agent != agent
    assert ^cookies = Agent.get(agent, & &1)

    assert [cookies: ^cookies, cookies_agent: ^agent, cookies_str: "1; 2; 3"] =
             :ets.tab2list(tid) |> Enum.sort()

    # set cookies and agent
    assert {%{tid: ^tid, agent: ^agent}, ^options} = SetCookie.prepare_for_start(nil, options)
    assert ^cookies = Agent.get(agent, & &1)

    assert [cookies: ^cookies, cookies_agent: ^agent, cookies_str: "1; 2; 3"] =
             :ets.tab2list(tid) |> Enum.sort()
  end

  test "when handling request" do
    tid = new_ets_table()
    state = %{tid: tid}

    # when empty
    request = %SpiderMan.Request{key: "/", url: "/", options: []}
    :ets.insert(tid, cookies_str: "")
    assert ^request = SetCookie.call(request, state)

    # when not empty
    cookies = "test=1; key1=a; key2=b"
    :ets.insert(tid, cookies_str: cookies)
    return_request = %{request | options: [headers: [{"cookie", cookies}]]}
    assert ^return_request = SetCookie.call(request, state)
    request = %{request | options: [headers: [{"cookie", "other=string; login=true"}]]}

    # already had cookie, append
    return_request = %{
      request
      | options: [headers: [{"cookie", cookies}, {"cookie", "other=string; login=true"}]]
    }

    assert ^return_request = SetCookie.call(request, state)
  end

  test "when handling response" do
    tid = new_ets_table()
    {state, _} = SetCookie.prepare_for_start(nil, common_pipeline_tid: tid)
    agent = state.agent

    # empty
    env = %Tesla.Env{headers: []}
    response = %SpiderMan.Response{key: "/", env: env}
    assert ^response = SetCookie.call(response, state)
    assert [] = Agent.get(agent, & &1)

    assert [cookies: [], cookies_agent: ^agent, cookies_str: ""] =
             :ets.tab2list(tid) |> Enum.sort()

    # only one
    env = %{env | headers: [{"set-cookie", "login=true; Path=/; HttpOnly; Secure; SameSite=Lax"}]}
    response = %{response | env: env}
    assert ^response = SetCookie.call(response, state)
    cookies = ["login=true"]
    assert ^cookies = Agent.get(agent, & &1)

    assert [cookies: ^cookies, cookies_agent: ^agent, cookies_str: "login=true"] =
             :ets.tab2list(tid) |> Enum.sort()

    # multiple
    env = %{
      env
      | headers: [
          {"set-cookie", "a=1; Path=/; HttpOnly"},
          {"set-cookie", "b=2; Path=/; HttpOnly"}
        ]
    }

    response = %{response | env: env}
    assert ^response = SetCookie.call(response, state)
    cookies = ["a=1", "b=2", "login=true"]
    assert ^cookies = Agent.get(agent, & &1)

    assert [cookies: ^cookies, cookies_agent: ^agent, cookies_str: "a=1; b=2; login=true"] =
             :ets.tab2list(tid) |> Enum.sort()

    # duplicate
    assert ^response = SetCookie.call(response, state)
    assert ^cookies = Agent.get(agent, & &1)

    assert [cookies: ^cookies, cookies_agent: ^agent, cookies_str: "a=1; b=2; login=true"] =
             :ets.tab2list(tid) |> Enum.sort()
  end

  defp new_ets_table, do: :ets.new(:set_cookie, [:set, :public])
end
