defmodule SpiderMan.Pipeline.DuplicateFilterTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Pipeline.DuplicateFilter

  test "prepare_for_start" do
    common_pipeline_tid = "common_pipeline_tid"
    pipeline_tid = "pipeline_tid"
    options = [common_pipeline_tid: common_pipeline_tid, pipeline_tid: pipeline_tid]

    assert {^common_pipeline_tid, ^options} = DuplicateFilter.prepare_for_start(:common, options)
    assert {^pipeline_tid, ^options} = DuplicateFilter.prepare_for_start(nil, options)
  end

  test "duplicate filter" do
    tid = :ets.new(:duplicate_filter, [:set, :public])
    event_1 = %{key: 1}
    event_2 = %{key: 2}

    # new one
    assert ^event_1 = DuplicateFilter.call(event_1, tid)
    assert ^event_2 = DuplicateFilter.call(event_2, tid)

    # duplicate filter
    assert :skiped = DuplicateFilter.call(event_1, tid)
    assert :skiped = DuplicateFilter.call(event_2, tid)
  end
end
