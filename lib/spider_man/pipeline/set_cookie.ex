defmodule SpiderMan.Pipeline.SetCookie do
  @moduledoc false
  require Logger
  alias SpiderMan.{Request, Response}
  @behaviour SpiderMan.Pipeline

  @impl true
  def call(%Response{env: env} = response, state) do
    env.headers
    |> Stream.filter(&match?({"set-cookie", _}, &1))
    |> Enum.map(fn {_, cookie} ->
      String.split(cookie, ";", parts: 2) |> hd()
    end)
    |> update_cookies(state)

    response
  end

  def call(%Request{options: options} = request, %{tid: tid}) do
    case :ets.lookup(tid, :cookies_str) do
      [{_, ""}] ->
        request

      [{_, cookies}] ->
        header = {"cookie", cookies}
        options = Keyword.update(options, :headers, [header], &[header | &1])
        %{request | options: options}
    end
  end

  @impl true
  def prepare_for_start(_arg, options) do
    tid = options[:common_pipeline_tid]
    {:ok, agent} = Agent.start_link(fn -> [] end)
    :ets.insert(tid, [{:cookies, []}, {:cookies_str, ""}])
    {%{tid: tid, agent: agent}, options}
  end

  defp update_cookies([], _state), do: :ok

  defp update_cookies(cookies, %{tid: tid, agent: agent}) do
    Agent.update(agent, fn old ->
      cookies = Enum.uniq(cookies ++ old)
      cookies_str = Enum.join(cookies, "; ")
      :ets.insert(tid, [{:cookies, cookies}, {:cookies_str, cookies_str}])
      cookies
    end)
  end
end
