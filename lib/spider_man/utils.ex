defmodule SpiderMan.Utils do
  @moduledoc false
  alias SpiderMan.{Request, Item}

  def build_requests(urls, options \\ [method: :get], max_retries \\ 3) do
    Enum.map(urls, &build_request(&1, options, max_retries))
  end

  def build_request(url, options \\ [method: :get], max_retries \\ 3) do
    %Request{key: url, url: url, options: options, retries: max_retries}
  end

  def build_items(items, options \\ [], max_retries \\ 1) do
    Enum.map(items, fn {key, value} ->
      build_item(key, value, options, max_retries)
    end)
  end

  def build_item(key, value, options \\ [], max_retries \\ 1) do
    %Item{key: key, value: value, options: options, retries: max_retries}
  end

  def merge_settings(old_settings, new_settings) do
    Keyword.merge(old_settings, new_settings, fn _k, v1, v2 ->
      Keyword.merge(v1, v2, fn
        :pipelines, _m1, m2 ->
          m2

        :batchers, _b1, b2 ->
          b2

        _sk, sv1, sv2 when is_list(sv1) and is_list(sv2) ->
          Keyword.merge(sv1, sv2)

        _sk, sv1, sv2 when is_map(sv1) and is_map(sv2) ->
          Map.merge(sv1, sv2)

        _sk, _sv1, sv2 ->
          sv2
      end)
    end)
  end

  def call_producer(broadway, msg) do
    [producer_name] = Broadway.producer_names(broadway)
    GenStage.call(producer_name, msg)
  end
end
