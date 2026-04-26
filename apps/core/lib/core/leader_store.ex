defmodule Core.LeaderStore do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{})
  end

  @impl true
  def init(_args) do
    table = :ets.new(
      :leader_store,
      [ :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: :auto ])

    {:ok, table }
  end
end
