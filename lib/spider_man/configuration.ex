defmodule SpiderMan.Configuration do
  @moduledoc false
  alias SpiderMan.{Utils, Pipeline.DuplicateFilter, Producer}

  @default_settings [
    downloader_options: [
      producer: Producer.ETS,
      processor: [max_demand: 1],
      rate_limiting: [allowed_messages: 10, interval: 1000],
      pipelines: [DuplicateFilter],
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
      storage: SpiderMan.Storage.JsonLines,
      pipelines: [DuplicateFilter],
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
      print_stats: [type: :boolean, default: true],
      log2file: [type: {:or, [:boolean, :string]}, default: true],
      status: [type: {:in, [:running, :suspended]}, default: :running],
      spider_module: [type: :atom],
      ets_file: [type: :string],
      downloader_options: [
        type: :keyword_list,
        keys:
          [
            requester: [
              type: {:or, [:atom, :mod_arg]},
              default: {{SpiderMan.Requester.Finch, []}}
            ]
          ] ++ component_spec(:downloader),
        subsection: "### Downloader options"
      ],
      spider_options: [
        type: :keyword_list,
        keys: component_spec(:spider),
        subsection: "### Spider options"
      ],
      item_processor_options: [
        type: :keyword_list,
        keys:
          [
            storage: [type: :atom, default: SpiderMan.Storage.JsonLines],
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
              subsection: "#### Batchers options"
            ]
          ] ++ component_spec(:item_processor),
        subsection: "### ItemProcessor options"
      ],
      *: [type: :any]
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
      default: []
    ]

    processor_spec = [
      type: :keyword_list,
      default: [],
      keys: [
        stages: [
          type: :pos_integer,
          deprecated: "Use :concurrency instead",
          rename_to: :concurrency
        ],
        concurrency: [type: :pos_integer, default: System.schedulers_online() * 2],
        min_demand: [type: :non_neg_integer],
        max_demand: [type: :non_neg_integer, default: 10],
        partition_by: [type: {:fun, 1}],
        spawn_opt: [type: :keyword_list],
        hibernate_after: [type: :pos_integer]
      ]
    ]

    rate_limiting_spec = [
      type: :non_empty_keyword_list,
      keys: [
        allowed_messages: [required: true, type: :pos_integer],
        interval: [required: true, type: :pos_integer]
      ]
    ]

    {processor_spec, rate_limiting_spec, pipelines_spec, post_pipelines_spec} =
      case component do
        :downloader ->
          {
            Keyword.put(processor_spec, :default, max_demand: 1),
            Keyword.put(rate_limiting_spec, :default, allowed_messages: 10, interval: 1000),
            Keyword.put(pipelines_spec, :default, [DuplicateFilter]),
            pipelines_spec
          }

        :spider ->
          {
            Keyword.put(processor_spec, :default, max_demand: 1),
            rate_limiting_spec,
            pipelines_spec,
            pipelines_spec
          }

        :item_processor ->
          {
            processor_spec,
            rate_limiting_spec,
            Keyword.put(pipelines_spec, :default, [DuplicateFilter]),
            pipelines_spec
          }
      end

    [
      producer: [type: {:or, [:atom, :mod_arg]}, default: Producer.ETS],
      context: [type: :any, default: %{}],
      processor: processor_spec,
      rate_limiting: rate_limiting_spec,
      pipelines: pipelines_spec,
      post_pipelines: post_pipelines_spec
    ]
  end

  def validate_pipeline(v = {fun, _arg}) when is_function(fun, 2), do: {:ok, v}
  def validate_pipeline(v = {mod, f, _arg}) when is_atom(mod) and is_atom(f), do: {:ok, v}
  def validate_pipeline(mod) when is_atom(mod), do: {:ok, mod}
  def validate_pipeline(v), do: {:error, "bad pipeline: #{inspect(v)}"}

  def validate_settings!(spider, spider_settings) do
    global_settings = Application.get_env(:spider_man, :global_settings, [])
    settings = Utils.merge_settings(@default_settings, global_settings)

    spider_module =
      Keyword.get_lazy(spider_settings, :spider_module, fn ->
        Keyword.get(settings, :spider_module, spider)
      end)

    with {:module, _} <- Code.ensure_loaded(spider_module),
         true <- function_exported?(spider_module, :settings, 0) do
      Utils.merge_settings(settings, spider_module.settings())
    else
      {:error, _} -> raise "Spider module: #{inspect(spider_module)} undefined!"
      false -> settings
    end
    |> Utils.merge_settings(spider_settings)
    |> Keyword.merge(spider: spider, spider_module: spider_module)
    |> NimbleOptions.validate!(configuration_spec())
  end
end
