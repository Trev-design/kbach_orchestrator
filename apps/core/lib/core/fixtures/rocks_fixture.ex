defmodule Core.Fixtures.RocksFixture do
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_args) do
    Process.flag(:trap_exit, true)
    cache_size = System.get_env("CACHE_SIZE", "512MB") |> Core.Application.parse_cache_string()
    { :ok, cache } = :rocksdb.new_cache(:lru, cache_size)
    { :ok, wbm } = :rocksdb.new_write_buffer_manager(cache_size, cache)
    num_partitions = String.to_integer(System.get_env("NUM_PARTITIONS", "8"))

    {:ok, [cache: cache, size: cache_size, id: "test", num_partitions: num_partitions, write_buffer_manager: wbm]}
  end

  @impl true
  def handle_call(:get, _from, state) do
    {:reply, state, state}
  end

  def get() do
    GenServer.call(__MODULE__, :get)
  end
end
