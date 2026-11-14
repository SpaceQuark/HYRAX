import { useEffect, useState } from "react";
import type { ClusterNode, Gpu } from "./types";

type Connection = "connecting" | "live" | "reconnecting";

function useCluster() {
  const [nodes, setNodes] = useState<ClusterNode[] | null>(null);
  const [connection, setConnection] = useState<Connection>("connecting");

  useEffect(() => {
    const source = new EventSource("/api/stream");
    source.addEventListener("nodes", (event) => {
      setNodes(JSON.parse((event as MessageEvent<string>).data));
      setConnection("live");
    });
    // EventSource retries on its own; this only updates the indicator.
    source.onerror = () => setConnection("reconnecting");
    return () => source.close();
  }, []);

  return { nodes, connection };
}

function Bar({ fraction }: { fraction: number }) {
  const percent = Math.min(100, Math.max(0, fraction * 100));
  return (
    <div className="bar">
      <div className="bar-fill" style={{ width: `${percent}%` }} />
    </div>
  );
}

function gib(mb: number) {
  return (mb / 1024).toFixed(1);
}

function GpuRow({ gpu }: { gpu: Gpu }) {
  return (
    <div className="gpu">
      <div className="gpu-head">
        <span className="gpu-index">{gpu.index}</span>
        <span className="gpu-name">{gpu.name.replace(/^NVIDIA (GeForce )?/, "")}</span>
        <span className="gpu-extra">
          {gpu.temperatureC ?? "–"}°C · {gpu.powerW == null ? "–" : Math.round(gpu.powerW)} W
        </span>
      </div>
      <div className="meter">
        <span className="meter-label">Util</span>
        <Bar fraction={(gpu.utilization ?? 0) / 100} />
        <span className="meter-value">{gpu.utilization ?? "–"}%</span>
      </div>
      <div className="meter">
        <span className="meter-label">Memory</span>
        <Bar fraction={gpu.memoryUsedMb / gpu.memoryTotalMb} />
        <span className="meter-value">
          {gib(gpu.memoryUsedMb)} / {gib(gpu.memoryTotalMb)} GiB
        </span>
      </div>
      {gpu.processes.length > 0 && (
        <table className="processes">
          <tbody>
            {gpu.processes.map((process) => (
              <tr key={`${process.pid}-${process.type}`} className={process.type}>
                <td className="num">{process.pid}</td>
                <td>{process.user ?? "?"}</td>
                <td className="num">{process.memoryMb ?? "?"} MiB</td>
                <td className="command" title={process.command ?? ""}>
                  {process.command ?? "?"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function NodeCard({ node }: { node: ClusterNode }) {
  return (
    <section className={node.online ? "node" : "node offline"}>
      <header className="node-head">
        <h2>{node.node}</h2>
        {node.online ? (
          <span className="badge online">online</span>
        ) : (
          <span className="badge">offline since {new Date(node.lastSeen).toLocaleTimeString()}</span>
        )}
      </header>
      {node.gpus.map((gpu) => (
        <GpuRow key={gpu.uuid} gpu={gpu} />
      ))}
    </section>
  );
}

export function App() {
  const { nodes, connection } = useCluster();
  const gpus = nodes?.flatMap((node) => (node.online ? node.gpus : [])) ?? [];

  return (
    <main>
      <header className="top">
        <h1>HYRAX</h1>
        {nodes && (
          <span className="summary">
            {nodes.filter((node) => node.online).length} of {nodes.length} nodes online · {gpus.length} GPUs
          </span>
        )}
        <span className={`connection ${connection}`}>{connection}</span>
      </header>
      {nodes?.length === 0 && <p className="empty">No agents have reported yet.</p>}
      <div className="nodes">
        {nodes?.map((node) => (
          <NodeCard key={node.node} node={node} />
        ))}
      </div>
    </main>
  );
}
