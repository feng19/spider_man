defmodule SpiderMan.StorageTest do
  use ExUnit.Case, async: true
  alias SpiderMan.{CommonSpider, Storage, Utils}

  setup_all do
    [spider: StorageTest]
  end

  @tag :tmp_dir
  test "prepare_for start and stop", %{tmp_dir: tmp_dir, spider: spider} do
    # set file_path
    storage = Storage.JsonLines
    file_path = Path.join(tmp_dir, "data_#{System.system_time(:millisecond)}.jsonl")

    options =
      Storage.prepare_for_start(
        storage: {storage, file_path},
        spider: spider,
        batchers: :not_empty
      )

    assert [
             spider: ^spider,
             batchers: :not_empty,
             storage: ^storage,
             context: %{
               storage: ^storage,
               storage_context: %{io_device: io_device, file_path: ^file_path}
             }
           ] = options

    assert :ok = Storage.prepare_for_stop(options)
    assert not Process.alive?(io_device)

    # unset file_path
    options = Storage.prepare_for_start(storage: storage, spider: spider, batchers: :not_empty)

    assert [
             spider: ^spider,
             batchers: :not_empty,
             storage: ^storage,
             context: %{storage: ^storage, storage_context: %{io_device: io_device, file_path: _}}
           ] = options

    assert :ok = Storage.prepare_for_stop(options)
    assert not Process.alive?(io_device)
  end

  test "multi storage", %{spider: spider} do
    options =
      Storage.prepare_for_start(
        storage: [Storage.JsonLines, Storage.Log],
        spider: spider,
        batchers: :not_empty
      )

    assert [
             spider: ^spider,
             batchers: :not_empty,
             storage: Storage.Multi,
             context: %{
               storage: Storage.Multi,
               storage_context: %{storage_list: storage_list}
             }
           ] = options

    assert [
             %{
               storage: Storage.JsonLines,
               storage_context: %{io_device: jsonl_io_device, file_path: jsonl_file_path}
             },
             %{storage: Storage.Log, storage_context: _}
           ] = storage_list

    assert File.exists?(jsonl_file_path)

    items = [Utils.build_item(1, %{val: "a"}), Utils.build_item(2, %{val: "b"})]
    assert :ok = Storage.Multi.store(:not_empty, items, %{storage_list: storage_list})

    assert :ok = Storage.prepare_for_stop(options)
    assert not Process.alive?(jsonl_io_device)
  end

  test "storage=false", %{spider: spider} do
    handle_response = fn _response, _context -> %{} end

    assert {:ok, _pid} =
             CommonSpider.start(spider, [handle_response: handle_response],
               item_processor_options: [storage: false]
             )

    assert %{item_processor_options: false, item_processor_pid: nil} = SpiderMan.get_state(spider)
    assert :ok = SpiderMan.suspend(spider)
    assert :ok = SpiderMan.continue(spider)
    assert :ok = SpiderMan.stop(spider)
  end
end
