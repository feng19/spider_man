defmodule SpiderMan.Pipeline.SetCookie do
  @moduledoc """
  auto save cookies for spider component & auto set cookie for downloader component

  ## Usage

      settings = [
        downloader_options: [
          pipelines: [#{inspect(__MODULE__)}]
        ],
        spider_options: [
          pipelines: [#{inspect(__MODULE__)}]
        ],
        ...
      ]
  """
  require Logger
  alias SpiderMan.{Request, Response}
  @behaviour SpiderMan.Pipeline

  @impl true
  # when handling request, call by downloader component
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

  # when handling response, call by spider component
  def call(%Response{env: env} = response, state) do
    env.headers
    |> Stream.filter(&match?({"set-cookie", _}, &1))
    |> Enum.map(fn {_, cookie} ->
      String.split(cookie, ";", parts: 2) |> hd()
    end)
    |> update_cookies(state)

    response
  end

  @impl true
  def prepare_for_start(_arg, options) do
    tid = options[:common_pipeline_tid]

    cookies =
      case :ets.lookup(tid, :cookies) do
        [{_, cookies}] ->
          cookies

        [] ->
          :ets.insert(tid, cookies: [], cookies_str: "")
          []
      end

    state =
      case :ets.lookup(tid, :cookies_agent) do
        [{_, agent}] ->
          state = %{tid: tid, agent: agent}
          update_cookies(cookies, %{tid: tid, agent: agent})
          state

        _ ->
          {:ok, agent} = Agent.start_link(fn -> cookies end)
          cookies_str = Enum.join(cookies, "; ")
          :ets.insert(tid, cookies_agent: agent, cookies_str: cookies_str)
          %{tid: tid, agent: agent}
      end

    {state, options}
  end

  defp update_cookies([], _state), do: :ok

  defp update_cookies(cookies, %{tid: tid, agent: agent}) do
    Agent.update(agent, fn old ->
      cookies = Enum.uniq(cookies ++ old)
      cookies_str = Enum.join(cookies, "; ")
      :ets.insert(tid, cookies: cookies, cookies_str: cookies_str)
      cookies
    end)
  end
end
