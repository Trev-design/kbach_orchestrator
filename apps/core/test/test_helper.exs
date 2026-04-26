ExUnit.start()

cache_size = System.get_env("CACHE_SIZE", "512MB") |> parse_cache_string()
{ :ok, cache } = :rocksdb.new_cache(:lru, cache_size)
{ :ok, wbm } = :rocksdb.new_write_buffer_manager(cache_size, cache)
num_partitions = String.to_integer(System.get_env("NUM_PARTITIONS", "8"))
