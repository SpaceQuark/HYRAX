"""NVML sampling. Field names match the heartbeat JSON the control plane expects."""

import os
import pwd

import pynvml

MIB = 1024 * 1024


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _optional(fn, *args):
    try:
        return fn(*args)
    except pynvml.NVMLError:
        return None


def _process_user(pid: int) -> str | None:
    # The OS user, which on shared-login machines is not the person.
    try:
        uid = os.stat(f"/proc/{pid}").st_uid
    except OSError:
        return None
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def _process_command(pid: int) -> str | None:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\0", b" ").decode(errors="replace").strip() or None
    except OSError:
        return None


def _processes(handle) -> list[dict]:
    found = []
    for kind, query in (
        ("compute", pynvml.nvmlDeviceGetComputeRunningProcesses),
        ("graphics", pynvml.nvmlDeviceGetGraphicsRunningProcesses),
    ):
        for proc in _optional(query, handle) or []:
            memory = proc.usedGpuMemory
            found.append(
                {
                    "pid": proc.pid,
                    "type": kind,
                    "user": _process_user(proc.pid),
                    "command": _process_command(proc.pid),
                    "memoryMb": None if memory is None else memory // MIB,
                }
            )
    return found


def sample() -> list[dict]:
    """One reading per GPU. Call pynvml.nvmlInit() first."""
    gpus = []
    for index in range(pynvml.nvmlDeviceGetCount()):
        handle = pynvml.nvmlDeviceGetHandleByIndex(index)
        # v2 excludes driver-reserved memory from `used`, which is what nvidia-smi shows.
        memory = pynvml.nvmlDeviceGetMemoryInfo(handle, version=pynvml.nvmlMemory_v2)
        utilization = _optional(pynvml.nvmlDeviceGetUtilizationRates, handle)
        power_mw = _optional(pynvml.nvmlDeviceGetPowerUsage, handle)
        gpus.append(
            {
                "index": index,
                "uuid": _text(pynvml.nvmlDeviceGetUUID(handle)),
                "name": _text(pynvml.nvmlDeviceGetName(handle)),
                "utilization": None if utilization is None else utilization.gpu,
                "memoryUsedMb": round(memory.used / MIB),
                "memoryTotalMb": memory.total // MIB,
                "temperatureC": _optional(
                    pynvml.nvmlDeviceGetTemperature, handle, pynvml.NVML_TEMPERATURE_GPU
                ),
                "powerW": None if power_mw is None else round(power_mw / 1000, 1),
                "processes": _processes(handle),
            }
        )
    return gpus
