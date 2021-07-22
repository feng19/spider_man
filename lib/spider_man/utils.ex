defmodule SpiderMan.Utils do
  @moduledoc "Utils"
  alias SpiderMan.{Request, Response, Item}

  @type url :: String.t()
  @type urls :: [url]
  @type options :: keyword
  @type max_retries :: integer
  @typep spider_man_struct :: Request.t() | Response.t() | Item.t()

  @doc "build requests"
  @spec build_requests(urls, options, max_retries) :: [Request.t()]
  def build_requests(urls, options \\ [method: :get], max_retries \\ 3) do
    Enum.map(urls, &build_request(&1, options, max_retries))
  end

  @doc "build a request"
  @spec build_request(url, options, max_retries) :: Request.t()
  def build_request(url, options \\ [method: :get], max_retries \\ 3) do
    %Request{key: url, url: url, options: options, retries: max_retries}
  end

  @doc "build items"
  @spec build_items(items :: [{key :: any, value :: any}], options, max_retries) :: [Item.t()]
  def build_items(items, options \\ [], max_retries \\ 1) do
    Enum.map(items, fn {key, value} ->
      build_item(key, value, options, max_retries)
    end)
  end

  @doc "build a item"
  @spec build_item(key :: any, value :: any, options, max_retries) :: Item.t()
  def build_item(key, value, options \\ [], max_retries \\ 1) do
    %Item{key: key, value: value, options: options, retries: max_retries}
  end

  @doc "set key for request|response|item"
  @spec set_key(spider_man_struct, key :: any) :: spider_man_struct
  def set_key(struct, key), do: %{struct | key: key}
  @doc "set flag for request|response|item"
  @spec set_flag(spider_man_struct, flag :: any) :: spider_man_struct
  def set_flag(struct, flag), do: %{struct | flag: flag}

  @doc false
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

  @doc false
  def ets_stream(table) do
    Stream.unfold(:ets.first(table), fn
      :"$end_of_table" ->
        nil

      key ->
        [record] = :ets.lookup(table, key)
        next_key = :ets.next(table, key)
        {record, next_key}
    end)
  end

  @doc false
  def get_file_path_by_spider(spider, suffix),
    do: "data/#{inspect(spider)}_#{System.system_time(:second)}.#{suffix}"

  @doc false
  def dump_ets2file(tid, file_name) do
    file_name = String.to_charlist(file_name)
    :ets.tab2file(tid, file_name, extended_info: [:md5sum], sync: true)
  end

  @doc false
  def setup_ets_from_file!(file_name) do
    file_name
    |> String.to_charlist()
    |> :ets.file2tab(verify: true)
    |> case do
      {:ok, tid} -> tid
      {:error, error} -> raise "setup_ets_from_file: #{file_name} error: #{inspect(error)}"
    end
  end
end
