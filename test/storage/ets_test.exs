defmodule SpiderMan.Storage.ETSTest do
  use ExUnit.Case, async: true
  alias SpiderMan.{Item, Storage.ETS}

  setup_all do
    [spider: ETSTest]
  end

  @tag :tmp_dir
  test "prepare_for start and stop", %{tmp_dir: tmp_dir, spider: spider} do
    # set file_path
    file_path = Path.join(tmp_dir, "data_#{System.system_time(:millisecond)}.ets")

    assert {%{tid: tid, file_path: ^file_path}, []} = ETS.prepare_for_start(file_path, [])

    assert :ok =
             ETS.prepare_for_stop(context: %{storage_context: %{tid: tid, file_path: file_path}})

    assert :undefined = :ets.info(tid)

    # unset file_path
    assert {%{tid: tid, file_path: file_path}, [spider: ^spider]} =
             ETS.prepare_for_start(nil, spider: spider)

    assert :ok =
             ETS.prepare_for_stop(context: %{storage_context: %{tid: tid, file_path: file_path}})

    assert :undefined = :ets.info(tid)

    # give_away
    testing_pid = self()
    proxy_pid = spawn_link(fn -> loop_receive(testing_pid) end)
    give_away = {proxy_pid, :stop_msg}

    assert {%{tid: tid, file_path: file_path, give_away: ^give_away}, [spider: ^spider]} =
             ETS.prepare_for_start([give_away: give_away], spider: spider)

    assert :ok =
             ETS.prepare_for_stop(
               context: %{
                 storage_context: %{tid: tid, file_path: file_path, give_away: give_away}
               }
             )

    assert_receive {:"ETS-TRANSFER", ^tid, ^testing_pid, :stop_msg}, 1000
    assert true = :ets.info(tid) |> is_list()
    send(proxy_pid, :stop)
    Process.sleep(50)
    assert not Process.alive?(proxy_pid)
    assert :undefined = :ets.info(tid)
  end

  test "store", %{spider: spider} do
    assert {storage_context, [spider: ^spider]} = ETS.prepare_for_start(nil, spider: spider)
    items = Enum.map(1..3, &%Item{key: &1, value: &1})
    assert :ok = ETS.store(:default, items, storage_context)
    assert [{1, 1}, {2, 2}, {3, 3}] = :ets.tab2list(storage_context.tid) |> Enum.sort()
  end

  defp loop_receive(testing_pid) do
    receive do
      :stop ->
        :ok

      info ->
        send(testing_pid, info)
        loop_receive(testing_pid)
    end
  end
end
