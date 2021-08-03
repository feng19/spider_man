defmodule SpiderMan.Application do
  @moduledoc false
  use Application
  alias SpiderMan.{Configuration, Engine}

  @app :spider_man
  @supervisor SpiderMan.Supervisor

  @impl true
  def start(_type, _args) do
    children =
      Application.get_env(@app, :spiders, [])
      |> Enum.map(fn spider ->
        {spider, settings} =
          case spider do
            {spider, settings} when is_atom(spider) and is_list(settings) ->
              {spider, settings}

            spider when is_atom(spider) ->
              {spider, []}
          end

        {Engine, Configuration.validate_settings!(spider, settings)}
      end)

    opts = [strategy: :one_for_one, name: @supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_child(spider, spider_settings \\ []) do
    {_, gl} = Process.info(self(), :group_leader)
    settings = Configuration.validate_settings!(spider, spider_settings)
    Supervisor.start_child(@supervisor, {Engine, [{:group_leader, gl} | settings]})
  end

  def stop_child(spider) do
    with :ok <- Supervisor.terminate_child(@supervisor, spider) do
      Supervisor.delete_child(@supervisor, spider)
    end
  end
end
