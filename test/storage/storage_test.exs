defmodule SpiderMan.StorageTest do
  use ExUnit.Case, async: true
  alias SpiderMan.{Storage, CommonSpider}

  setup_all do
    File.rm_rf("data")
    on_exit(fn -> File.rm_rf("data") end)
    [storage: Storage.JsonLines, spider: StorageTest]
  end

  @tag :tmp_dir
  test "prepare_for start and stop", %{tmp_dir: tmp_dir, storage: storage, spider: spider} do
    # set file_path
    file_path = Path.join(tmp_dir, "data_#{System.system_time(:second)}.jsonl")

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

  test "storage=false", %{spider: spider} do
    handle_response = fn _response, _context -> %{} end

    assert {:ok, _pid} =
             CommonSpider.start(spider, [handle_response: handle_response],
               item_processor_options: [storage: false]
             )

    assert %{item_processor_options: false, item_processor_pid: nil} = SpiderMan.get_state(spider)
  end
end
