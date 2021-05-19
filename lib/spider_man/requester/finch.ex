defmodule SpiderMan.Requester.Finch do
  @moduledoc false
  alias SpiderMan.Requester
  @behaviour Requester

  @default_options [
    spec_options: [pools: %{:default => [size: 32, count: 8]}],
    adapter_options: [pool_timeout: 5_000],
    request_options: [receive_timeout: 10_000],
    append_default_middlewares?: true,
    middlewares: [],
    logging?: false
  ]

  @impl true
  def request(url, options, context) do
    options = Keyword.merge(context.request_options, options)

    context.middlewares
    |> Tesla.client({Tesla.Adapter.Finch, context.adapter_options})
    |> Tesla.request([{:url, url} | options])
  end

  @impl true
  def prepare_for_start(finch_options, downloader_options) do
    finch_options = finch_options || []
    spider = Keyword.fetch!(downloader_options, :spider)
    finch_name = :"#{spider}.Finch"

    finch_options =
      @default_options
      |> Keyword.merge(finch_options)
      |> handle_proxy_option()

    request_options = finch_options[:request_options]
    finch_spec = {Finch, [{:name, finch_name} | finch_options[:spec_options]]}
    adapter_options = [{:name, finch_name} | finch_options[:adapter_options]]

    middlewares =
      Requester.append_default_middlewares(
        finch_options[:append_default_middlewares?],
        finch_options
      )

    context = %{adapter_options: adapter_options, middlewares: middlewares}

    producer =
      case Keyword.fetch!(downloader_options, :producer) do
        {producer, producer_options} ->
          producer_options =
            Keyword.update(producer_options, :additional_specs, [finch_spec], &[finch_spec | &1])

          {producer, producer_options}

        producer ->
          {producer, [additional_specs: [finch_spec]]}
      end

    downloader_options
    |> Keyword.put(:producer, producer)
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

  def default_options, do: @default_options

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
end
