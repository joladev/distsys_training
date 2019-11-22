defmodule Shortener.LinkManager.Cache do
  @moduledoc false
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def lookup(cache \\ __MODULE__, key) do
    case :ets.lookup(cache, key) do
      [{_key, value}] -> {:ok, value}
      _ -> {:error, :not_found}
    end
  end

  def insert(cache \\ __MODULE__, key, value) do
    GenServer.call(cache, {:insert, key, value})
  end

  def broadcast_insert(cache \\ __MODULE__, key, value) do
    GenServer.abcast(Node.list(), cache, {:insert, key, value})
  end

  def flush(cache \\ __MODULE__) do
    GenServer.call(cache, :flush)
  end

  def init(args) do
    # TODO - Replace nil with real table
    _ = :ets.new(__MODULE__, [:public, :named_table])
    {:ok, %{table: __MODULE__}}
  end

  def handle_cast({:insert, key, value}, data) do
    _ = :ets.insert(__MODULE__, {key, value})
    {:noreply, data}
  end

  def handle_call({:insert, key, value}, _from, data) do
    _ = :ets.insert(__MODULE__, {key, value})
    {:reply, :ok, data}
  end

  def handle_call(:flush, _from, data) do
    :ets.delete_all_objects(data.table)
    {:reply, :ok, data}
  end
end
