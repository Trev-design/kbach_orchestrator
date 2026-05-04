File.mkdir_p!("./store/test")
{:ok, _pid} = Core.Fixtures.RocksPayload.start_link([])
ExUnit.start()
ExUnit.after_suite(fn _ -> File.rm_rf!("./store/test") end)
