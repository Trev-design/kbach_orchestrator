defmodule Core.Utils.LibFormat do
  alias Core.Utils.LibId
  alias Core.CoreTypes

  @spec format_batch_commands(any(), any()) :: {:ok, list()} | {:ok, list(), list()}
  def format_batch_commands(command_list, state) do
    command_list
    |> group_batch_commands()
    |> fetch_group_ids(state)
    |> Stream.concat()
    |> format()
    |> get_entry_data()
  end

  def format_results(results), do: get_entry_data(results)

  def format_key(group, state, id_fn) do
    with {:ok, group_id} <- LibId.get_group_id(state.db_handle, state.cf_handles["meta"], group),
         {:ok, full_id}  <- id_fn.(group_id)
    do
      {:ok, full_id}
    else
      {:error, reason} -> {:error, "Failed to format seek command: #{reason}"}
    end
  end

  defp group_batch_commands(command_list) do
    command_list
    |> Enum.group_by(
      fn %CoreTypes.BatchType{} = batch -> batch.key_data.group_id end,
      &{&1.command, &1.key_data.tenant_id, &1.key_data.job_id, &1.value, &1.cf_name})
  end

  defp fetch_group_ids(command_list, state) do
    command_list
    |> Stream.map(fn {group, commands} ->
      case LibId.get_group_id(state.db_handle, state.cf_handles["meta"], group) do
        {:ok, group_id} -> Stream.map(commands, fn {command, tenant_id, job_id, value, cf_name} ->
          if value != nil do
            {:ok, {{command, {tenant_id, group_id, job_id}, value}, cf_name}}
          else
            {:ok, {{command, {tenant_id, group_id, job_id}}, cf_name}}
          end
        end)
        {:error, reason} -> {:error, "Failed to get group ID for group #{group}: #{reason}"}
      end
    end)
  end

  defp format(command_list) do
    command_list
    |> Enum.map(fn value ->
      case value do
        {:ok, cmd_tuple} -> format_batch_command(cmd_tuple)
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  defp format_batch_command(cmd_tuple) do
    case cmd_tuple do
      {{command, {tenant_id, group_id, job_id}, value}, cf_name} ->
        case LibId.get_full_id(tenant_id, group_id, job_id) do
          {:ok, full_id} -> {:ok, {{command, full_id, value}, cf_name}}
          {:error, reason} -> {:error, "Failed to generate full ID for #{tenant_id}, #{group_id}, #{job_id}: #{reason}"}
        end

      {{command, {tenant_id, group_id, job_id}}, cf_name} ->
        case LibId.get_full_id(tenant_id, group_id, job_id) do
          {:ok, full_id} -> {:ok, {{command, full_id}, cf_name}}
          {:error, reason} -> {:error, "Failed to generate full ID for #{tenant_id}, #{group_id}, #{job_id}: #{reason}"}
        end

      _ -> {:error, "Invalid command tuple format: #{inspect(cmd_tuple)}"}
    end
  end

  defp get_entry_data(entries) do
    cleaned_ids =
      entries
      |> Stream.filter(fn value -> match?({:ok, _}, value) end)
      |> Enum.map(fn {:ok, id} -> id end)

    if length(cleaned_ids) == length(entries) do
      {:ok, cleaned_ids}
    else
      errors =
        entries
        |> Stream.filter(fn value -> match?({:error, _}, value) end)
        |> Enum.map(fn {:error, reason} -> reason end)

      {:ok, cleaned_ids, errors}
    end
  end
end
