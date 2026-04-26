defmodule Core.StateMachineStarter do
  alias Core.StateMachine.ShardSupervisor

  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    :ok = make_shard(args)
    {:ok, %{}}
  end

  defp make_shard([
    cache_size: cache_size,
    cache: cache,
    wbm: wbm,
    num_partitions: num_partitions,
    num_nodes: num_nodes
  ]) do
    Enum.each(1..num_partitions, fn id ->
      DynamicSupervisor.start_child(
        Core.RaftSupervisor, {
          ShardSupervisor,
          [
            id: id,
            cache_size: cache_size,
            num_partitions: num_partitions,
            lru_cache: cache,
            write_buffer_manager: wbm,
            num_nodes: num_nodes
          ]})
    end)
  end
end
