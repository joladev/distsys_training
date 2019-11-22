defmodule Shortener.LinkManager do
  @moduledoc """
  Manages the lifecycles of links
  """

  alias Shortener.{Storage, LinkManager}
  alias Shortener.LinkManager.Cache
  alias Shortener.Cluster

  @lookup_sup __MODULE__.LookupSupervisor

  def child_spec(_args) do
    children = [
      Cache,
      # TODO - Extend this supervision tree to support remote lookups
    ]

    %{
      id: __MODULE__,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  def create(url) do
    short_code = generate_short_code(url)

    Storage.set(short_code, url)
    node = Cluster.find_node(short_code)
    case :rpc.call(node, Cache, :insert, [short_code, url]) do
      {:badrpc, :nodedown} -> {:error, :node_down}
      _ -> {:ok, short_code}
    end
  end

  def lookup(short_code) do
    with {:ok, url} <- Cache.lookup(short_code) do
      {:ok, url}
    else
      {:error, :not_found} -> check_storage(short_code)
    end
  end

  defp check_storage(short_code) do
    with {:ok, url} <- Storage.get(short_code) do
      Cache.insert(short_code, url)
      {:ok, url}
    end
  end

  def remote_lookup(short_code) do
    # TODO - Do a remote lookup
    node = Cluster.find_node(short_code)
    case :rpc.call(node, LinkManager, :lookup, [short_code]) do
      {:badrpc, :nodedown} -> {:error, :node_down}
      {:ok, url} -> {:ok, url}
    end
  end

  def generate_short_code(url) do
    url
    |> hash
    |> Base.encode16(case: :lower)
    |> String.to_integer(16)
    |> pack_bitstring
    |> Base.url_encode64
    |> String.replace(~r/==\n?/, "")
  end

  defp hash(str), do: :crypto.hash(:sha256, str)

  defp pack_bitstring(int), do: << int :: big-unsigned-32 >>
end
