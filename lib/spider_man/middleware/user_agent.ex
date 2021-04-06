defmodule SpiderMan.Middleware.UserAgent do
  @moduledoc false
  @behaviour Tesla.Middleware

  @impl true
  def call(env, next, user_agents) do
    user_agent = Enum.random(user_agents)

    env
    |> Tesla.put_headers([{"user-agent", user_agent}])
    |> Tesla.run(next)
  end
end
