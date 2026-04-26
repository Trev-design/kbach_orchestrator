defmodule Core.LeaderStoreLib do
  require Ex2ms
  def insert_leader(id, leader_node) do
    :ets.insert_new(:leader_store, {id, leader_node, 0})
  end

  def update_leader(id, leader_node, term) do
    match_spec = Ex2ms.fun do {leader_id, _node, row_term} when leader_id == ^id and row_term < ^term ->
      {leader_id, ^leader_node, ^term}
    end

    :ets.select_replace(:leader_store, match_spec)
  end

  def get_leader(id) do
    :ets.lookup(:leader_store, id)
  end
end
