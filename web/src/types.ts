// Mirrors ClusterState.Node and Heartbeat.Gpu on the server.

export interface GpuProcess {
  pid: number;
  type: "compute" | "graphics";
  user: string | null;
  command: string | null;
  memoryMb: number | null;
}

export interface Gpu {
  index: number;
  uuid: string;
  name: string;
  utilization: number | null;
  memoryUsedMb: number;
  memoryTotalMb: number;
  temperatureC: number | null;
  powerW: number | null;
  processes: GpuProcess[];
}

export interface GpuFlags {
  reserved: boolean;
  // Set once a process has held memory on an idle GPU for a while.
  baskingSince: string | null;
}

export interface ClusterNode {
  node: string;
  agentVersion: string;
  lastSeen: string;
  online: boolean;
  gpus: Gpu[];
  flags: Record<string, GpuFlags>;
}
