defmodule SpiderMan.Storage.CSVTest do
  use ExUnit.Case, async: true
  alias SpiderMan.{Item, Storage.CSV}

  setup_all do
    [spider: CSVTest]
  end

  @tag :tmp_dir
  test "prepare_for start and stop", %{tmp_dir: tmp_dir, spider: spider} do
    # set file_path
    file_path = Path.join(tmp_dir, "data_#{System.system_time(:millisecond)}.csv")
    headers = [:a, :b]

    assert {%{io_device: io_device, file_path: ^file_path, header_keys: ^headers}, []} =
             CSV.prepare_for_start([file: file_path, headers: headers], [])

    assert :ok = CSV.prepare_for_stop(context: %{storage_context: %{io_device: io_device}})
    assert not Process.alive?(io_device)

    # unset file_path
    assert {%{io_device: io_device, file_path: _}, [spider: ^spider]} =
             CSV.prepare_for_start([headers: headers], spider: spider)

    assert :ok = CSV.prepare_for_stop(context: %{storage_context: %{io_device: io_device}})
    assert not Process.alive?(io_device)
  end

  test "store", %{spider: spider} do
    assert {storage_context, [spider: ^spider]} =
             CSV.prepare_for_start([headers: [:a, :b]], spider: spider)

    items = Enum.map(1..3, &%Item{key: &1, value: %{a: &1, b: &1 * 2}})
    assert :ok = CSV.store(:default, items, storage_context)
    assert "a,b\r\na,b\r\n1,2\r\n2,4\r\n3,6\r\n" = File.read!(storage_context.file_path)
  end
end
