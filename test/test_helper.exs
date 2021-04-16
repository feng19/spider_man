System.put_env("TMPDIR", "tmp")
ExUnit.start(seed: 0)
SpiderMan.Modules.start_agent(5)
