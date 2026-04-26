defmodule Core.Job do
  alias Core.LeaderStoreLib

  defstruct [:job, :id, :command, :state, priority: :low]

  def process_job(%__MODULE__{} = job), do: make_job(job, 5, 0)
  def process_job_with_max_tries(%__MODULE__{} = job, max_tries), do: make_job(job, max_tries, 0)

  defp make_job(
    %__MODULE__{
    job: job,
    id: id} = new_job,
    max_tries,
    num_retries,
    leader \\ nil)
  do
    if num_retries < max_tries do
      job_id = "#{job}:#{id}"
      partition_id = generate_partition_id(job_id)

      case get_leader(leader, partition_id) do
        {:ok, leader} ->
          try_make_job(new_job, max_tries, num_retries, partition_id, leader)

        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :max_tries_reached}
    end
  end

  defp try_make_job(
    %__MODULE__{
    job: job,
    command: command,
    priority: priority} = new_job,
    max_tries,
    num_retries,
    partition_id,
    {leader, term})
  do
    with correlation_id <- make_ref(),
         :ok            <- :ra.pipeline_command({partition_id, leader}, {job, command}, correlation_id, priority),
         {:ok, reply}   <- receive_message(correlation_id)
    do
      {:ok, reply}
    else
      {:error, :timeout} = response ->
        response

      {:error, :undefined_leader} ->
        {:error, :undefined_leader, partition_id}

      {:error, :not_leader, current_leader} ->
        retry_with_updated_leader(new_job, partition_id, current_leader, term, max_tries, num_retries)
    end
  end

  defp get_leader(leader, partition_id) do
    if leader == nil, do: get_leader_from_db(partition_id), else: {:ok, leader}
  end

  defp get_leader_from_db(partition_id) do
    case LeaderStoreLib.get_leader(partition_id) do
      [{_, leader_node, term}] -> {:ok, {leader_node, term}}
      []                       -> {:error, :not_found}
    end
  end

  defp retry_with_updated_leader(
    %__MODULE__{} = job,
    partition_id,
    current_leader,
    term,
    max_retries,
    num_retries)
  do
    new_term = term + 1
    LeaderStoreLib.update_leader(partition_id, current_leader, new_term)
    make_job(job, max_retries, num_retries + 1, {current_leader, new_term})
  end

  defp generate_partition_id(job_id) do
    num_partitions = :persistent_term.get({Core.Globals,:num_partitions})

    "partition_#{:erlang.phash2(job_id, num_partitions)}"
    |> String.to_existing_atom()
  end

  defp receive_message(correlation_id) do
    receive do
      {:ra_event, _leader, {:applied, [{^correlation_id, reply}]}} ->
        {:ok, reply}

      {:ra_event, _from, {:rejected, {:not_leader, :undefined, ^correlation_id}}} ->
        {:error, :undefined_leader}

      {:ra_event, _from, {:rejected, {:not_leader, leader, ^correlation_id}}} ->
        {:error, :not_leader, leader}
    after
      10000 ->
        {:error, :timeout}
    end
  end
end
