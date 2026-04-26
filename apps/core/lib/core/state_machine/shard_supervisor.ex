defmodule Core.StateMachine.ShardSupervisor do
  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args)
  end

  @impl true
  def init([
    id: id,
    cache_size: size,
    num_partitions: num_partitions,
    lru_cache: cache,
    write_buffer_manager: wbm,
    num_nodes: num_nodes
  ]) do
    children = [
      { Core.StateMachine.RocksOwner, [
        cache: cache,
        size: size,
        num_partitions: num_partitions,
        write_buffer_manager: wbm] },

      { Core.StateMachine.RaftServer, [
        partition_id: id,
        num_nodes: num_nodes] }
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
