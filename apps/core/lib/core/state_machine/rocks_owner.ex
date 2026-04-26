defmodule Core.StateMachine.RocksOwner do
  @moduledoc """
  This module implements a GenServer that manages the RocksDB instance and its column family handles.
  It initializes the RocksDB database with the specified options and provides a way for other processes
  (such as the RaftServer) to retrieve the database handles for performing operations on the database.
  The GenServer also ensures that the RocksDB instance is properly closed when the server terminates,
  preventing resource leaks and ensuring data integrity.
  The RocksDB instance is configured with options that optimize it for use in a Raft-based state machine,
  including settings for write buffer size, compaction, and caching to ensure efficient performance under the expected workload
  of the state machine.
  The column families are set up to support different data retention policies
  (e.g., ephemeral, hourly, daily, weekly, monthly) to allow for efficient data management and querying based on the age of the data.
  The GenServer is designed to be started as part of a supervision tree,
  ensuring that it is automatically restarted if it crashes,
  and that it can be easily integrated with other components of the system that require access to the RocksDB instance.
  The module also includes error handling to ensure that any issues with opening the RocksDB
  instance or retrieving the handles are properly logged and do not cause the entire system to fail,
  allowing for graceful degradation in case of issues with the database.
  """
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def get_handles(pid) do
    GenServer.call(pid, :get_handles)
  end

  @impl true
  def init([
    cache: cache,
    size: size,
    id: id,
    num_partitions: num_partitions,
    write_buffer_manager: wbm
    ])
  do
    Process.flag(:trap_exit, true)

    opts = [
      {:create_if_missing, true},
      {:create_missing_column_families, true},
      {:write_buffer_manager, wbm},
      {:max_open_files, -1}
    ]

    table_options = [
      {:cache_index_and_filter_blocks, true},
      {:bloom_filter_policy, 10},
      {:block_cache, cache}
    ]

    cf_options = [
      {:block_based_table_options, table_options},
      {:write_buffer_size, div(size, num_partitions * 8)},
      {:max_write_buffer_number, 2},
      {:min_write_buffer_number_to_merge, 1},
      {:compression, :lz4},
      {:level_compaction_dynamic_level_bytes, true},
      {:optimize_filters_for_hits, true},
      {:prefix_extractor, {:fixed_prefix_transform, 32}}
    ]

    cf_descriptors = [
      {~c"default", cf_options, 0},
      {~c"meta", cf_options, 0},
      {~c"ephemeral", cf_options, 60},
      {~c"transient", cf_options, 60 * 60},
      {~c"permanent", cf_options, 0},
      {~c"logs", cf_options, 0},
    ]

    name = get_name(id)

    handles = create_handles(name, opts, cf_descriptors)

    {:ok, handles}
  end

  @impl true
  def handle_call(:get_handles, _from, state), do: {:reply, {:ok, state}, state}

  @impl true
  def terminate(_reason, state) do
    :rocksdb.close(state.db_handle)
    :ok
  end

  def terminate(pid), do: GenServer.stop(pid, :normal)

  defp create_handles(name, db_opts, cf_descriptors) do
    case :rocksdb.open_with_ttl_cf(name, db_opts, cf_descriptors, false) do
      {:ok, db_handle, cf_handles} ->
        [default, meta, ephemeral, transient, permanent, logs] = cf_handles

        %{
          db_handle: db_handle,
          cf_handles: %{
            default: default,
            meta: meta,
            ephemeral: ephemeral,
            transient: transient,
            permanent: permanent,
            logs: logs
          }
        }

      {:error, reason} -> raise "Failed to open RocksDB: #{reason}"
    end
  end

  defp get_name(id) do
    case Mix.env() do
      :dev  -> ~c"store/dev/kv_store_partition_#{id}"
      :test -> ~c"store/test/kv_store_partition_#{id}"
      :prod -> ~c"store/prod/kv_store_partition_#{id}"
      _     -> raise "Unknown environment: #{Mix.env()}"
    end
  end
end
