defmodule Core.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    cache_size = System.get_env("CACHE_SIZE", "512MB") |> parse_cache_string()
    { :ok, cache } = :rocksdb.new_cache(:lru, cache_size)
    { :ok, wbm } = :rocksdb.new_write_buffer_manager(cache_size, cache)
    num_partitions = String.to_integer(System.get_env("NUM_PARTITIONS", "8"))
    num_nodes = String.to_integer(System.get_env("NUM_NODES", "1"))
    :persistent_term.put({Core.Globals,:num_partitions}, num_partitions)

    :ra_system.default_config()
    |> Map.merge(
      %{
      name: :orchestrator_system,
      wal_max_size_bytes: 536870912,
      wal_max_entries: 25600,
      wal_sync_method: :datasync,
      wal_compute_checksums: true,
      wal_max_batch_size: 16384,
      wal_hibernate_after: 2000,
      data_dir: "/mnt/ra_data",
      wal_data_dir: "/mnt/ra_wal"})
    |> :ra_system.start()

    children = [
      {Registry, keys: :unique, name: Core.RaftRegistry},
      {DynamicSupervisor, name: Core.RaftSupervisor, strategy: :one_for_one},
      {Core.StateMachineStarter, [
        cache_size: cache_size,
        cache: cache,
        wbm: wbm,
        num_partitions: num_partitions,
        num_nodes: num_nodes]}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :rest_for_one, name: Core.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def parse_cache_string(string) do
    clean_string = string |> String.upcase() |> String.replace(" ", "")

    case Regex.named_captures(~r/^(?<num>\d+(\.\d+)?)(?<unit>GB|MB|KB|B)?$/, clean_string) do
      nil -> raise "Invalid cache size format: #{string}"

      %{"num" => num_str, "unit" => unit} ->
        num = if String.contains?(num_str, ".") do
          String.to_float(num_str)
        else
          String.to_integer(num_str)
        end

        if unit == "", do: num, else: calculate_cache_size(unit, num)
    end
  end

  defp calculate_cache_size(unit, num) do
    case unit do
      "GB" -> round(num * 1024 * 1024 * 1024)
      "MB" -> round(num * 1024 * 1024)
      "KB" -> round(num * 1024)
      "B"  -> round(num)
    end
  end
end
