defmodule SpiderMan.Pipeline.DuplicateFilterTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Pipeline.DuplicateFilter

  test "prepare_for_start" do
    common_pipeline_tid = "common_pipeline_tid"
    pipeline_tid = "pipeline_tid"
    options = [common_pipeline_tid: common_pipeline_tid, pipeline_tid: pipeline_tid, spider: nil]

    assert {{^common_pipeline_tid, nil}, ^options} =
             DuplicateFilter.prepare_for_start(:common, options)

    assert {{^pipeline_tid, nil}, ^options} = DuplicateFilter.prepare_for_start(nil, options)
  end

  test "duplicate filter" do
    tid = :ets.new(:duplicate_filter, [:set, :public])
    arg = {tid, nil}
    event_1 = %{key: 1}
    event_2 = %{key: 2}

    # new one
    assert ^event_1 = DuplicateFilter.call(event_1, arg)
    assert ^event_2 = DuplicateFilter.call(event_2, arg)

    # duplicate filter
    assert :skiped = DuplicateFilter.call(event_1, arg)
    assert :skiped = DuplicateFilter.call(event_2, arg)
  end
end
