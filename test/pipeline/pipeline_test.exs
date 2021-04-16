defmodule SpiderMan.PipelineTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog
  alias SpiderMan.Pipeline

  test "prepare_for_start/2" do
    # empty pipelines
    pipelines = []
    options = []
    assert {^pipelines, []} = Pipeline.prepare_for_start(pipelines, options)

    add_one_fun_1 = &(&1 + 1)
    add_one_fun_2 = &(&1 + &2)
    module = Pipeline.Standard
    pipeline_arg = [:any_thing]
    mfa = {module, :call, pipeline_arg}
    options = []

    # fun/1
    pipelines = [add_one_fun_1]
    assert {^pipelines, ^options} = Pipeline.prepare_for_start(pipelines, options)

    # fun/2 by fun
    pipelines = [add_one_fun_2]
    assert {[{^add_one_fun_2, nil}], ^options} = Pipeline.prepare_for_start(pipelines, options)

    # fun/2 by {fun, arg}
    pipelines = [{add_one_fun_2, 1}]
    assert {^pipelines, ^options} = Pipeline.prepare_for_start(pipelines, options)

    # m
    pipelines = [module]
    assert {[{^module, :call, nil}], []} = Pipeline.prepare_for_start(pipelines, options)
    pipelines = [Pipeline.OnlyCall]

    assert {[{Pipeline.OnlyCall, :call, nil}], []} =
             Pipeline.prepare_for_start(pipelines, options)

    # {m, arg}
    pipelines = [{module, pipeline_arg}]
    assert {[^mfa], []} = Pipeline.prepare_for_start(pipelines, options)

    # {m, f, arg}
    pipelines = [{module, :call, pipeline_arg}]
    assert {^pipelines, []} = Pipeline.prepare_for_start(pipelines, options)
    pipelines = [{Pipeline.NoCallFunction, :run, pipeline_arg}]
    assert {^pipelines, []} = Pipeline.prepare_for_start(pipelines, options)

    # undefined module
    assert_raise RuntimeError, "Pipeline module: :unfound_module undefined!", fn ->
      pipelines = [:unfound_module]
      Pipeline.prepare_for_start(pipelines, options)
    end

    # undefined function
    module = Pipeline.Empty

    assert_raise RuntimeError,
                 "Pipeline module: #{inspect(module)} undefined function call/2!",
                 fn ->
                   pipelines = [module]
                   Pipeline.prepare_for_start(pipelines, options)
                 end

    module = Pipeline.NoCallFunction

    assert_raise RuntimeError,
                 "Pipeline module: #{inspect(module)} undefined function not_found_function/2!",
                 fn ->
                   pipelines = [{module, :not_found_function, []}]
                   Pipeline.prepare_for_start(pipelines, options)
                 end
  end

  test "call/2" do
    spider = PipelineTest
    add_one_fun_1 = &(&1 + 1)
    add_one_fun_2 = {&(&1 + &2), 1}
    module = SpiderMan.Pipeline.JsonEncode

    # empty pipelines
    pipelines = []
    assert [] = Pipeline.call(pipelines, [], spider)

    # fun/1
    pipelines = [add_one_fun_1]
    assert 1 = Pipeline.call(pipelines, 0, spider)
    pipelines = [add_one_fun_1, add_one_fun_1]
    assert 2 = Pipeline.call(pipelines, 0, spider)

    # fun/2 by {fun, arg}
    pipelines = [add_one_fun_2]
    assert 1 = Pipeline.call(pipelines, 0, spider)
    pipelines = [add_one_fun_2, add_one_fun_2]
    assert 2 = Pipeline.call(pipelines, 0, spider)

    # {m, f, arg}
    pipelines = [{module, :call, []}]
    item = %SpiderMan.Item{key: 1, value: %{test: 1}}
    assert ~S|{"test":1}| = Pipeline.call(pipelines, item, spider)
    pipelines = [{module, :call, [pretty: true]}]
    assert "{\n  \"test\": 1\n}" = Pipeline.call(pipelines, item, spider)

    pipelines = [
      &(&1 + 1),
      {&(&1 + &2), 1},
      &%SpiderMan.Item{key: 1, value: &1},
      {module, :call, []}
    ]

    assert "2" = Pipeline.call(pipelines, 0, spider)

    # exit
    assert capture_log([level: :error], fn ->
             pipelines = [fn _ -> exit(:normal) end]
             assert {:error, :normal} = Pipeline.call(pipelines, 0, spider)
           end) =~ "(exit) normal"

    assert capture_log([level: :error], fn ->
             pipelines = [fn _ -> exit({:test, :reason}) end]
             assert {:error, {:test, :reason}} = Pipeline.call(pipelines, 0, spider)
           end) =~ "(exit) {:test, :reason}"

    # raise
    assert capture_log([level: :error], fn ->
             pipelines = [fn _ -> raise "test" end]
             assert {:error, %RuntimeError{message: "test"}} = Pipeline.call(pipelines, 0, spider)
           end) =~ "(RuntimeError) test"

    assert capture_log([level: :error], fn ->
             error = %ArgumentError{message: "reason"}
             pipelines = [fn _ -> raise ArgumentError, "reason" end]
             assert {:error, ^error} = Pipeline.call(pipelines, 0, spider)
           end) =~ "(ArgumentError) reason"
  end

  test "prepare_for_stop/1" do
    # empty pipelines
    pipelines = []
    assert :ok = Pipeline.prepare_for_stop(pipelines)
    pipelines = [Pipeline.Empty, Pipeline.Standard, Pipeline.OnlyCall]
    assert :ok = Pipeline.prepare_for_stop(pipelines)
  end
end
