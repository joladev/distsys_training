defmodule Shortener.Aggregates do
  use GenServer

  alias __MODULE__
  alias Shortener.GCounter

  require Logger

  def count_for(table \\ __MODULE__, hash) do
    # TODO: Do lookup from ets in the client process
    case :ets.lookup(table, hash) do
      [{_, value}] -> value
      _ -> 0
    end
  end

  def increment(server \\ __MODULE__, hash) do
    GenServer.cast(server, {:increment, hash})
  end

  def merge(server \\ __MODULE__, hash, counter) do
    GenServer.cast(server, {:merge, hash, counter})
  end

  def flush(server \\ __MODULE__) do
    GenServer.call(server, :flush)
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args \\ []) do
    # TODO: Monitor node connections and disconnects
    :net_kernel.monitor_nodes(true)
    :ets.new(__MODULE__, [:named_table])

    {:ok, %{table: __MODULE__, counters: %{}}}
  end

  def handle_cast({:increment, short_code}, %{counters: counters}=data) do
    # TODO: Increment counter and broadcast a merge to the other nodes
    counters = Map.update(counters, short_code, GCounter.increment(GCounter.new()), &GCounter.increment/1)
    counter = Map.get(counters, short_code)
    current_count = GCounter.to_i(counter)
    :ets.insert(__MODULE__, {short_code, current_count})

    GenServer.abcast(Aggregates, {:merge, short_code, counter})

    {:noreply, %{data | counters: counters}}
  end

  def handle_cast({:merge, short_code, counter}, %{counters: counters} = data) do
    # TODO: Merge our existing set of counters with the new counter
    existing_counter = Map.get(counters, short_code) || GCounter.new()
    merged = GCounter.merge(counter, existing_counter)
    current_count = GCounter.to_i(merged)
    :ets.insert(__MODULE__, {short_code, current_count})

    data = %{data | counters: Map.put(counters, short_code, merged)}
    {:noreply, data}
  end

  def handle_call(:flush, _from, data) do
    :ets.delete_all_objects(data.table)
    {:reply, :ok, %{data | counters: %{}}}
  end

  def handle_info({:nodeup, node}, %{counters: counters} = data) do
    for {short_code, counter} <- counters do
      :rpc.call(node, Aggregates, :merge, [short_code, counter])
    end

    {:noreply, data}
  end

  def handle_info(msg, data) do
    # TODO - Handle node disconnects and reconnections
    Logger.info("Unhandled message: #{inspect msg}")

    {:noreply, data}
  end
end

