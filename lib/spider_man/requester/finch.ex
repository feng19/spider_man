defmodule SpiderMan.Requester.Finch do
  @moduledoc false
  alias Tesla.Middleware.{BaseUrl, Retry}
  @behaviour SpiderMan.Requester

  @impl true
  def request(url, options, context) do
    options = Keyword.merge(context.request_options, options)

    context.middlewares
    |> Tesla.client({Tesla.Adapter.Finch, context.adapter_options})
    |> Tesla.request([{:url, url} | options])
  end

  @impl true
  def prepare_for_start(finch_options, downloader_options) do
    spider = Keyword.fetch!(downloader_options, :spider)
    finch_name = :"#{spider}.Finch"

    finch_options =
      [
        spec_options: [pools: %{:default => [size: 32, count: 8]}],
        adapter_options: [pool_timeout: 5_000],
        request_options: [receive_timeout: 10_000],
        append_default_middlewares?: true,
        middlewares: [],
        logging?: false
      ]
      |> Keyword.merge(finch_options)
      |> handle_proxy_option()

    request_options = finch_options[:request_options]
    finch_spec = {Finch, [{:name, finch_name} | finch_options[:spec_options]]}
    adapter_options = [{:name, finch_name} | finch_options[:adapter_options]]

    middlewares =
      append_default_middlewares(finch_options[:append_default_middlewares?], finch_options)

    context = %{adapter_options: adapter_options, middlewares: middlewares}

    downloader_options
    |> Keyword.update(:additional_specs, [finch_spec], &[finch_spec | &1])
    |> Keyword.update(
      :context,
      Map.put(context, :request_options, request_options),
      fn old_context ->
        old_context
        |> Map.merge(context)
        |> Map.update(:request_options, request_options, &(request_options ++ &1))
      end
    )
  end

  def handle_proxy_option(finch_options) do
    case Keyword.get(finch_options, :proxy) do
      nil ->
        finch_options

      %{schema: schema, address: address} = setting when schema in [:http, :https] ->
        port =
          case schema do
            :http -> Map.get(setting, :port, 8080)
            :https -> Map.get(setting, :port, 443)
          end

        proxy = {schema, address, port, []}
        spec_options = Keyword.get(finch_options, :spec_options, [])
        pools = Keyword.get(spec_options, :pools, %{})

        proxy_headers =
          case Map.get(setting, :username) do
            nil ->
              []

            username ->
              password = Map.get(setting, :password, "")
              credentials = Base.encode64("#{username}:#{password}")
              [{"proxy-authorization", "Basic #{credentials}"}]
          end

        conn_opts = [proxy: proxy, proxy_headers: proxy_headers]

        pools =
          Map.new(pools, fn {name, pool_settings} ->
            {name,
             Keyword.update(pool_settings, :conn_opts, conn_opts, &Keyword.merge(&1, conn_opts))}
          end)

        spec_options = Keyword.put(spec_options, :pools, pools)
        Keyword.put(finch_options, :spec_options, spec_options)
    end
  end

  def append_default_middlewares(false, finch_options), do: finch_options[:middlewares]

  def append_default_middlewares(true, finch_options) do
    middlewares =
      if base_url = finch_options[:base_url] do
        [{BaseUrl, base_url} | finch_options[:middlewares]]
      else
        finch_options[:middlewares]
      end

    middlewares =
      if finch_options[:logging?] do
        middlewares ++ [Tesla.Middleware.Logger]
      else
        middlewares
      end

    if not_found_middleware?(middlewares, Retry) do
      retry_options = [
        delay: 500,
        max_retries: 3,
        max_delay: 4_000,
        should_retry: fn
          {:ok, %{status: status}} when status in [400, 500] -> true
          {:ok, _} -> false
          {:error, _} -> true
        end
      ]

      [{Retry, retry_options} | middlewares]
    else
      middlewares
    end
  end

  defp not_found_middleware?(middlewares, middleware) do
    Enum.all?(middlewares, fn
      {^middleware, _} -> false
      ^middleware -> false
      _ -> true
    end)
  end
end
