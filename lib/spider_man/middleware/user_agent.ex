defmodule SpiderMan.Middleware.UserAgent do
  @moduledoc """
  Setting user-agent for request

  ## Usage

      settings = [
        downloader_options: [requester: {SpiderMan.Requester.Finch, [
          middlewares: [
          {#{inspect(__MODULE__)},
           [
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4389.82 Safari/537.36",
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4389.82 Safari/537.36",
             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
           ]}],
           ...
        ]}]
      ]
  """
  @behaviour Tesla.Middleware

  @impl true
  def call(env, next, user_agents) do
    user_agent = Enum.random(user_agents)

    env
    |> Tesla.put_headers([{"user-agent", user_agent}])
    |> Tesla.run(next)
  end
end
