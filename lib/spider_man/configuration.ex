defmodule SpiderMan.Configuration do
  alias SpiderMan.{Pipeline, Producer, Storage, Utils}

  @default_settings [
    downloader_options: [
      producer: Producer.ETS,
      processor: [max_demand: 1],
      rate_limiting: [allowed_messages: 10, interval: 1000],
      pipelines: [Pipeline.DuplicateFilter],
      post_pipelines: [],
      context: %{}
    ],
    spider_options: [
      producer: Producer.ETS,
      processor: [max_demand: 1],
      pipelines: [],
      post_pipelines: [],
      context: %{}
    ],
    item_processor_options: [
      producer: Producer.ETS,
      storage: Storage.JsonLines,
      pipelines: [Pipeline.DuplicateFilter],
      post_pipelines: [],
      context: %{},
      batchers: [
        default: [
          concurrency: 1,
          batch_size: 50,
          batch_timeout: 1000
        ]
      ]
    ]
  ]

  @moduledoc """
  Handle settings for spider

  ## Startup Spiders

      config :spider_man, :spiders, [
        SpiderA,
        {SpiderB, settings = [...]},
        ...
      ]

  All Spider what defined on `:spiders` would auto startup while the `:spider_man` application started.

  ## Global Settings

      config :spider_man, global_settings: settings = [...]

  This `settings` work for all spiders.

  ## Settings for Spider on config files

      config :spider_man, SpiderA, settings = [...]

  This `settings` only work for `SpiderA`.

  ## Default Settings

  ```elixir
  #{inspect(@default_settings, pretty: true)}
  ```

  ## Settings Priority

  1. Settings for Spider directly.
    1.1 `settings` defined in `spiders` for the Spider.
    1.2 As second argument while call `SpiderMan.start/2`.
  2. Return by callback function: `SpiderModule.settings/0`.
  3. Settings for Spider on config files.
  4. Global Settings.
  5. Default Settings.
  """

  def configuration_docs do
    configuration_spec()
    |> Keyword.delete(:*)
    |> NimbleOptions.docs()
  end

  def configuration_spec do
    batcher_keys_spec = [
      concurrency: [type: :pos_integer, default: 1],
      batch_size: [type: :pos_integer, default: 100],
      batch_timeout: [type: :pos_integer, default: 1000],
      partition_by: [type: {:fun, 1}],
      spawn_opt: [type: :keyword_list],
      hibernate_after: [type: :pos_integer]
    ]

    [
      print_stats: [type: :boolean, default: true, doc: "Print the stats of spider, "],
      log2file: [type: {:or, [:boolean, :string]}, default: true, doc: "Save the log to files, "],
      status: [
        type: {:in, [:running, :suspended]},
        default: :running,
        doc: "Set the startup status for the spider, "
      ],
      spider: [type: :atom, doc: "Set the callback module for the spider, "],
      spider_module: [type: :atom, doc: "Set the callback module for the spider, "],
      callbacks: [type: :keyword_list],
      ets_file: [
        type: :string,
        doc: "Set the filename for the spider, and load spider's state from ets files."
      ],
      downloader_options: [
        type: :keyword_list,
        keys:
          [
            requester: [
              type: {:or, [:atom, :mod_arg]},
              default: {SpiderMan.Requester.Finch, []}
            ]
          ] ++ component_spec(:downloader),
        doc: "see [Downloader Options](#t:settings/0-downloader-options).",
        subsection: "### Downloader options"
      ],
      spider_options: [
        type: :keyword_list,
        keys: component_spec(:spider),
        doc: "see [Spider Options](#t:settings/0-spider-options).",
        subsection: "### Spider options"
      ],
      item_processor_options: [
        type: :keyword_list,
        keys:
          [
            storage: [
              type: {:or, [:atom, :mod_arg]},
              default: Storage.JsonLines,
              doc: "Set a storage module what are store items, "
            ],
            batchers: [
              type: :keyword_list,
              default: [
                default: [
                  concurrency: 1,
                  batch_size: 50,
                  batch_timeout: 1000
                ]
              ],
              keys: [*: [type: :keyword_list, keys: batcher_keys_spec]],
              doc:
                "See [Batchers Options](https://hexdocs.pm/broadway/Broadway.html#start_link/2-batchers-options), ",
              subsection: "#### Batchers options"
            ]
          ] ++ component_spec(:item_processor),
        doc: "see [ItemProcessor Options](#t:settings/0-itemprocessor_options).",
        subsection: "### ItemProcessor options"
      ]
    ]
  end

  defp component_spec(component) do
    pipelines_spec = [
      type:
        {:list,
         {:or,
          [
            :mod_arg,
            {:fun, 1},
            {:fun, 2},
            {:custom, __MODULE__, :validate_pipeline, []}
          ]}},
      default: [],
      doc: "Each msg will handle by each pipelines, "
    ]

    processor_spec = [
      type: :keyword_list,
      default: [],
      keys: [
        concurrency: [type: :pos_integer, default: System.schedulers_online() * 2],
        min_demand: [type: :non_neg_integer],
        max_demand: [type: :non_neg_integer, default: 10],
        partition_by: [type: {:fun, 1}],
        spawn_opt: [type: :keyword_list],
        hibernate_after: [type: :pos_integer]
      ],
      doc:
        "See [Processors Options](https://hexdocs.pm/broadway/Broadway.html#start_link/2-processors-options), "
    ]

    rate_limiting_spec = [
      type:
        {:or,
         [
           {:non_empty_keyword_list,
            allowed_messages: [required: true, type: :pos_integer],
            interval: [required: true, type: :pos_integer]},
           nil
         ]},
      doc:
        "See [Producers Options - rate_limiting](https://hexdocs.pm/broadway/Broadway.html#start_link/2-producers-options), "
    ]

    {processor_spec, rate_limiting_spec, pipelines_spec} =
      case component do
        :downloader ->
          {
            Keyword.put(processor_spec, :default, max_demand: 1),
            Keyword.put(rate_limiting_spec, :default, allowed_messages: 10, interval: 1000),
            Keyword.put(pipelines_spec, :default, [Pipeline.DuplicateFilter])
          }

        :spider ->
          {
            Keyword.put(processor_spec, :default, max_demand: 1),
            rate_limiting_spec,
            pipelines_spec
          }

        :item_processor ->
          {
            processor_spec,
            rate_limiting_spec,
            Keyword.put(pipelines_spec, :default, [Pipeline.DuplicateFilter])
          }
      end

    [
      producer: [type: {:or, [:atom, :mod_arg]}, default: Producer.ETS],
      context: [type: :any, default: %{}],
      processor: processor_spec,
      rate_limiting: rate_limiting_spec,
      pipelines: pipelines_spec,
      post_pipelines: pipelines_spec
    ]
  end

  def validate_pipeline(v = {fun, _arg}) when is_function(fun, 2), do: {:ok, v}
  def validate_pipeline(v = {mod, f, _arg}) when is_atom(mod) and is_atom(f), do: {:ok, v}
  def validate_pipeline(mod) when is_atom(mod), do: {:ok, mod}
  def validate_pipeline(v), do: {:error, "bad pipeline: #{inspect(v)}"}

  def validate_settings!(spider, spider_settings) do
    global_settings = Application.get_env(:spider_man, :global_settings, [])
    local_settings = Application.get_env(:spider_man, spider, [])

    settings =
      @default_settings
      |> Utils.merge_settings(global_settings)
      |> Utils.merge_settings(local_settings)

    #      |> IO.inspect()

    spider_module =
      Keyword.get_lazy(spider_settings, :spider_module, fn ->
        Keyword.get(settings, :spider_module, spider)
      end)

    with {:module, _} <- Code.ensure_loaded(spider_module),
         true <- function_exported?(spider_module, :settings, 0) do
      Utils.merge_settings(settings, spider_module.settings())
    else
      {:error, _} ->
        raise "could not load module: #{inspect(spider_module)} for spider:#{inspect(spider)}!"

      false ->
        settings
    end
    |> Utils.merge_settings(spider_settings)
    #    |> IO.inspect()
    |> Keyword.merge(spider: spider, spider_module: spider_module)
    |> NimbleOptions.validate!(configuration_spec())
  end
end
