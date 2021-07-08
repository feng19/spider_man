# SpiderMan

[![Hex.pm](https://img.shields.io/hexpm/v/spider_man.svg?maxAge=2592000)](https://hex.pm/packages/wechat_sdk)
[![Hex Docs](https://img.shields.io/badge/hex-docs-9768d1.svg)](https://hexdocs.pm/spider_man)
[![Hex.pm](https://img.shields.io/hexpm/dt/spider_man.svg?maxAge=2592000)](https://hex.pm/packages/wechat_sdk)

**SpiderMan,a fast high-level web crawling & scraping framework for Elixir.**

inspired by [Crawly](https://github.com/elixir-crawly/crawly)(Elixir) and [Scrapy]()(Python).

## Installation

adding `spider_man` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:spider_man, "~> 0.2"}
  ]
end
```

## Usage

This example show how to create a module for crawly data 
from [elixir-jobs](https://elixirjobs.net/) website by use SpiderMan.

```elixir
Mix.install([
  {:spider_man, "~> 0.2"},
  {:floki, "~> 0.31"},
  {:nimble_csv, "~> 1.1"}
])

defmodule SpiderList.ElixirJobs do
  @moduledoc false
  use SpiderMan
  require Logger
  alias SpiderMan.Response

  @base_url "https://elixirjobs.net/"

  def run do
    SpiderMan.run_until_zero(__MODULE__, [], 3_000)
  end

  @impl true
  def settings do
    requester_options = [
      base_url: @base_url,
      middlewares: [
        {SpiderMan.Middleware.UserAgent,
         [
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4389.82 Safari/537.36",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4389.82 Safari/537.36",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
         ]},
        {Tesla.Middleware.Headers,
         [
           {"referer", @base_url},
           {"accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"},
           {"accept-encoding", "gzip, deflate"},
           {"accept-language", "zh-CN,zh;q=0.9,zh-TW;q=0.8,en;q=0.7"}
         ]},
        Tesla.Middleware.DecompressResponse
      ]
    ]

    [
      downloader_options: [requester: {SpiderMan.Requester.Finch, requester_options}],
      spider_options: [pipelines: []],
      item_processor_options: [
        storage: [
          {SpiderMan.Storage.ETS, "./data/jobs.ets"},
          {SpiderMan.Storage.CSV,
           file: "./data/jobs.csv",
           headers: [
             :link,
             :title,
             :sub_title,
             :date,
             :workplace,
             :type
           ]}
        ]
      ]
    ]
  end

  @impl true
  def init(state) do
    build_request(@base_url)
    |> set_flag(:first_page)
    |> then(&SpiderMan.insert_request(__MODULE__, &1))

    state
  end

  @impl true
  def handle_response(%Response{env: env, flag: :first_page}, _context) do
    total_page =
      Regex.run(~r/Showing page 1 of (\d+)/, env.body, capture: :all_but_first)
      |> hd()
      |> String.to_integer()

    Logger.info("total: #{total_page}")

    requests =
      Enum.map(2..total_page, fn n ->
        build_request("/?page=#{n}")
        |> set_flag({:list_page, n})
      end)

    handle_list_page(env.body, 1)
    |> Map.put(:requests, requests)
  end

  def handle_response(%Response{env: env, flag: {:list_page, n}}, _context) do
    handle_list_page(env.body, n)
  end

  defp handle_list_page(body, n) do
    Logger.info("processing page #{n}")
    {:ok, document} = Floki.parse_document(body)

    jobs =
      Floki.find(document, ".offers-index")
      |> hd()
      |> Floki.children(include_text: false)
      |> Enum.filter(&match?({"a", _, _}, &1))

    items =
      Enum.map(jobs, fn job ->
        title = Floki.find(job, ".title strong") |> Floki.text() |> String.trim()
        sub_title = Floki.find(job, ".title small") |> Floki.text() |> String.trim()
        link = Floki.attribute(job, "a", "href") |> hd()

        [_, date, _, workplace, _, type] =
          Floki.find(job, ".control .tag")
          |> Enum.map(&(&1 |> Floki.text() |> String.trim()))

        build_item(
          link,
          %{
            link: @base_url <> String.slice(link, 1..-1),
            title: title,
            sub_title: sub_title,
            date: date,
            workplace: workplace,
            type: type
          }
        )
      end)

    %{items: items}
  end
end

SpiderList.ElixirJobs.run()
```

copy this script and save to `elixir_jobs.exs` and then start by command:
```shell
elixir elixir_jobs.exs
```

## License

`spider_man` source code is released under Apache 2 License. Check the [LICENSE](./LICENSE) file for more information.