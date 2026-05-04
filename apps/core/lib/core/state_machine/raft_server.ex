defmodule Core.StateMachine.RaftServer do
  @moduledoc """
  This module holds a GenServer that initializes a Raft cluster using the :ra library.
  It retrieves the RocksDB handles from the RocksOwner GenServer and uses them to set up the Raft machine state.
  The Raft cluster is configured to run on all nodes in the cluster, with each node hosting a Raft server instance.
  The Raft machine is implemented in the RocksHandler module, which defines how to apply log entries to the state machine
  using RocksDB operations.
  """
  use GenServer

  alias Core.StateMachine.RocksOwner
  alias Core.Statemachine.RocksHandler

  def start_link([id: id, num_nodes: _num_nodes] = args) do
    GenServer.start_link(__MODULE__, args, name: {:via, Registry, {Core.RaftRegistry, "raft_server_#{id}"}})
  end

  @impl true
  def init([
    id: id,
    num_nodes: num_nodes
  ]) do
    db_handles =
      Process.info(self(), :parent)
      |> elem(1)
      |> Supervisor.which_children()
      |> Enum.find_value(fn {RocksOwner, pid, _, _} -> pid end)
      |> get_handles()

    {:ok, %{}, {:continue, {:setup_cluster, id, num_nodes, db_handles}}}
  end

  @impl true
  def handle_continue({:setup_cluster, id, num_nodes, db_handles}, _state) do
    {:ok, server_ids, machine} = make_cluster_config(id, db_handles)
    {:ok, cluster_state} = make_cluster(num_nodes, server_ids, machine)
    {:noreply, cluster_state}
  end

  # build necessary configuration for Raft cluster, including server IDs and machine state
  defp make_cluster_config(id, db_handles) do
    partition_id = "partition_#{id}" |> String.to_atom()

    server_ids = get_server_ids(partition_id)

    # the machine makes use of the RocksDB handles to perform operations on the database as part of Raft log application
    machine = {
      :module,
      RocksHandler,
      %{db_handle: db_handles.db_handle,
        cf_handles: db_handles.cf_handles,
        index: 0,
        max_index: 1000}}

    {:ok, server_ids, machine}
  end

  defp make_cluster(num_nodes, server_ids, machine) do
    init_cluster(server_ids, num_nodes, machine)
  end

  defp init_cluster(server_ids, num_nodes, machine) when length(server_ids) == num_nodes do
    if List.first(server_ids) |> elem(1) == Node.self() do
      case :ra.start_cluster(:orchestrator_system, :orchestrator, machine, server_ids) do
        {:ok, _, _} -> {:ok, %{server_ids: server_ids, machine: machine}}
        {:error, reason} -> raise "Failed to start Raft cluster: #{inspect(reason)}"
      end
    end
  end

  defp init_cluster(server_ids, num_nodes, machine), do: make_cluster(num_nodes, server_ids, machine)

  defp get_server_ids(partition_id) do
    [Node.self() | Node.list()]
    |> Enum.sort()
    |> Enum.map(fn node -> {partition_id, node} end)
  end

  defp get_handles(partner_pid) do
    if partner_pid == nil do
      raise "RocksOwner process not found"
    else
      RocksOwner.get_handles(partner_pid)
    end
  end
end
