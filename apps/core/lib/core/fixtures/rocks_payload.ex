defmodule Core.Fixtures.RocksPayload do
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_) do
    cache_size = 512 * 1024 * 1024 # 512 MB
    { :ok, cache } = :rocksdb.new_cache(:lru, cache_size)
    { :ok, wbm } = :rocksdb.new_write_buffer_manager(cache_size, cache)
    {:ok, [cache: cache, size: cache_size, num_partitions: 1, write_buffer_manager: wbm]}
  end

  def get_payload() do
    GenServer.call(__MODULE__, :get_payload)
  end

  @impl true
  def handle_call(:get_payload, _from, [
    cache: cache,
    size: cache_size,
    num_partitions: 1,
    write_buffer_manager: wbm] = state)
  do
    {:reply, [cache: cache, size: cache_size, id: :erlang.unique_integer([:monotonic, :positive]), num_partitions: 1, write_buffer_manager: wbm], state}
  end
end
