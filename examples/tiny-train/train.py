"""A small but real training run: a CNN learns to tell apart four kinds of synthetic texture.

No dataset to download, so it runs anywhere. It needs a GPU, holds a few GiB of VRAM, and writes a
checkpoint and metrics to HYRAX_OUTPUT_DIR, which `hyrax fetch <job>` brings back.

    hyrax run --vram 8G -f -- python train.py --epochs 5
"""

import argparse
import json
import os
import socket
import time
from pathlib import Path

import torch
from torch import nn


def textures(count: int, size: int, generator: torch.Generator, device: str):
    """Images of stripes at one of four angles, with random frequency and phase, under heavy noise."""
    labels = torch.randint(0, 4, (count,), generator=generator, device=device)
    angle = labels.float() * (torch.pi / 4)
    frequency = 4 + 8 * torch.rand(count, generator=generator, device=device)
    phase = 2 * torch.pi * torch.rand(count, generator=generator, device=device)
    axis = torch.linspace(-1, 1, size, device=device)
    y, x = torch.meshgrid(axis, axis, indexing="ij")
    along = x[None] * torch.cos(angle)[:, None, None] + y[None] * torch.sin(angle)[:, None, None]
    images = torch.sin(frequency[:, None, None] * torch.pi * along + phase[:, None, None])
    images = images + 1.5 * torch.randn(images.shape, generator=generator, device=device)
    return images[:, None].repeat(1, 3, 1, 1), labels


def block(inputs: int, outputs: int) -> nn.Sequential:
    return nn.Sequential(
        nn.Conv2d(inputs, outputs, 3, padding=1), nn.BatchNorm2d(outputs), nn.ReLU(),
        nn.Conv2d(outputs, outputs, 3, padding=1), nn.BatchNorm2d(outputs), nn.ReLU(), nn.MaxPool2d(2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--steps", type=int, default=60, help="training steps per epoch")
    parser.add_argument("--batch", type=int, default=256)
    parser.add_argument("--size", type=int, default=96)
    parser.add_argument("--width", type=int, default=96)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    if not torch.cuda.is_available():
        raise SystemExit("no GPU visible: this run is meant to be placed on one by hyrax")
    device = "cuda"
    name = torch.cuda.get_device_name(0)
    print(f"host {socket.gethostname()}, GPU {name}, torch {torch.__version__}, "
          f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES')}")

    torch.manual_seed(args.seed)
    generator = torch.Generator(device=device).manual_seed(args.seed)
    width = args.width
    model = nn.Sequential(
        block(3, width), block(width, 2 * width), block(2 * width, 4 * width),
        nn.AdaptiveAvgPool2d(1), nn.Flatten(), nn.Linear(4 * width, 4)).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=2e-3, weight_decay=1e-2)
    schedule = torch.optim.lr_scheduler.OneCycleLR(optimizer, max_lr=2e-3, total_steps=args.epochs * args.steps)
    scaler = torch.amp.GradScaler()
    held_out = textures(2048, args.size, torch.Generator(device=device).manual_seed(10_000), device)

    history = []
    started = time.time()
    for epoch in range(1, args.epochs + 1):
        model.train()
        running = 0.0
        for _ in range(args.steps):
            images, labels = textures(args.batch, args.size, generator, device)
            with torch.autocast("cuda", dtype=torch.bfloat16):
                loss = nn.functional.cross_entropy(model(images), labels)
            optimizer.zero_grad(set_to_none=True)
            scaler.scale(loss).backward()
            scaler.step(optimizer)
            scaler.update()
            schedule.step()
            running += loss.item()
        model.eval()
        with torch.no_grad(), torch.autocast("cuda", dtype=torch.bfloat16):
            accuracy = sum((model(held_out[0][i:i + 512]).argmax(1) == held_out[1][i:i + 512]).sum().item()
                           for i in range(0, 2048, 512)) / 2048
        history.append({"epoch": epoch, "train_loss": running / args.steps, "val_accuracy": accuracy,
                        "seconds": round(time.time() - started, 1)})
        print(f"epoch {epoch}/{args.epochs}  loss {running / args.steps:.4f}  val accuracy {accuracy:.3f}  "
              f"peak VRAM {torch.cuda.max_memory_allocated() / 2**20:.0f} MiB")

    outputs = Path(os.environ.get("HYRAX_OUTPUT_DIR", "outputs"))
    outputs.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), outputs / "model.pt")
    (outputs / "metrics.json").write_text(json.dumps({
        "host": socket.gethostname(), "gpu": name, "torch": torch.__version__, "args": vars(args),
        "train_seconds": round(time.time() - started, 1),
        "peak_vram_mib": round(torch.cuda.max_memory_allocated() / 2**20),
        "parameters": sum(p.numel() for p in model.parameters()),
        "history": history,
    }, indent=2) + "\n")
    print(f"wrote model.pt and metrics.json to {outputs}")


if __name__ == "__main__":
    main()
