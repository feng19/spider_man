defmodule SpiderMan.Storage.JsonLinesTest do
  use ExUnit.Case, async: true
  alias SpiderMan.Storage.JsonLines

  setup_all do
    File.rm_rf("data")
    on_exit(fn -> File.rm_rf("data") end)
  end

  @tag :tmp_dir
  test "prepare_for start and stop", %{tmp_dir: tmp_dir} do
    # set file_path
    file_path = Path.join(tmp_dir, "data_#{System.system_time(:second)}.jsonl")

    assert [context: %{storage_context: %{io_device: io_device, file_path: ^file_path}}] =
             JsonLines.prepare_for_start(file_path, [])

    assert :ok = JsonLines.prepare_for_stop(context: %{storage_context: %{io_device: io_device}})
    assert not Process.alive?(io_device)

    # unset file_path
    spider = JsonLinesTest

    assert [context: %{storage_context: %{io_device: io_device, file_path: _}}, spider: ^spider] =
             JsonLines.prepare_for_start(nil, spider: spider)

    assert :ok = JsonLines.prepare_for_stop(context: %{storage_context: %{io_device: io_device}})
    assert not Process.alive?(io_device)
  end

  test "store" do
    spider = JsonLinesTest

    [context: %{storage_context: storage_context}, spider: ^spider] =
      JsonLines.prepare_for_start(nil, spider: spider)

    items = Enum.map(1..3, &%SpiderMan.Item{key: &1, value: &1})
    assert [:ok, :ok, :ok] = JsonLines.store(:default, items, storage_context)
    assert "1\n2\n3\n" = File.read!(storage_context.file_path)
  end
end
