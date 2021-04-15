defmodule SpiderMan.Application do
  @moduledoc false
  use Application
  alias SpiderMan.Utils

  @supervisor SpiderMan.Supervisor

  @impl true
  def start(_type, _args) do
    children =
      Application.get_env(:spider_man, :spiders, [])
      |> Enum.map(fn spider ->
        {spider, settings} =
          case spider do
            {spider, settings} when is_atom(spider) and is_list(settings) ->
              {spider, settings}

            spider when is_atom(spider) ->
              settings = Application.get_env(:spider_man, spider, [])
              {spider, settings}
          end

        settings = merge_settings(spider, settings)
        spider_module = Keyword.fetch!(settings, :spider_module)
        spider_child_spec(spider, spider_module, settings)
      end)

    opts = [strategy: :one_for_one, name: @supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_child(spider, spider_settings \\ []) do
    settings = merge_settings(spider, spider_settings)
    spider_module = Keyword.fetch!(settings, :spider_module)
    child_spec = spider_child_spec(spider, spider_module, settings)
    Supervisor.start_child(@supervisor, child_spec)
  end

  def stop_child(spider) do
    with :ok <- Supervisor.terminate_child(@supervisor, spider) do
      Supervisor.delete_child(@supervisor, spider)
    end
  end

  defp spider_child_spec(spider, spider_module, settings) do
    %{
      id: spider,
      start: {spider_module, :start_link, [settings]},
      type: :supervisor
    }
  end

  defp merge_settings(spider, spider_settings) do
    default_settings = SpiderMan.default_settings()
    global_settings = Application.get_env(:spider_man, :global_settings, [])
    settings = Utils.merge_settings(default_settings, global_settings)

    spider_module =
      Keyword.get_lazy(spider_settings, :spider_module, fn ->
        Keyword.get(settings, :spider_module, spider)
      end)

    settings =
      with {:module, _} <- Code.ensure_loaded(spider_module),
           true <- function_exported?(spider_module, :settings, 0) do
        Utils.merge_settings(settings, spider_module.settings())
      else
        {:error, _} -> raise "Spider module: #{inspect(spider_module)} undefined!"
        false -> settings
      end
      |> Utils.merge_settings(spider_settings)

    Keyword.merge(settings, spider: spider, spider_module: spider_module)
  end
end
