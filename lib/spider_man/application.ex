defmodule SpiderMan.Application do
  @moduledoc false
  use Application
  alias SpiderMan.Utils

  @supervisor SpiderMan.Supervisor

  @impl true
  def start(_type, _args) do
    children =
      Application.get_env(:spider_man, :spiders, [])
      |> Enum.map(fn
        {spider, spider_settings} when is_atom(spider) ->
          settings = merge_settings(spider, spider_settings)
          {spider, settings}

        spider when is_atom(spider) ->
          spider_settings = Application.get_env(:spider_man, spider, [])
          settings = merge_settings(spider, spider_settings)
          {spider, settings}
      end)

    opts = [strategy: :one_for_one, name: @supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_child(spider, spider_settings \\ []) do
    settings = merge_settings(spider, spider_settings)
    Supervisor.start_child(@supervisor, {spider, settings})
  end

  def stop_child(spider) do
    Supervisor.terminate_child(@supervisor, spider)
  end

  defp merge_settings(spider, spider_settings) do
    default_settings = SpiderMan.default_settings()
    global_settings = Application.get_env(:spider_man, :global_settings, [])
    settings = Utils.merge_settings(default_settings, global_settings)

    with {:module, _} <- Code.ensure_loaded(spider),
         function_exported?(spider, :settings, 0) do
      Utils.merge_settings(settings, spider.settings())
    else
      {:error, _} -> raise "Spider module: #{inspect(spider)} undefined!"
      false -> settings
    end
    |> Utils.merge_settings(spider_settings)
  end
end
