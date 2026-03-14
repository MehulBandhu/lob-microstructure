import logging
import os
import shutil
import tarfile
from datetime import date, timedelta
from pathlib import Path

import zstandard as zstd

import config

logger = logging.getLogger(__name__)

def compress_day(target_date: date) -> Path:
    day_dir = config.TICK_DIR / str(target_date)
    if not day_dir.exists():
        logger.warning(f"No data directory for {target_date}")
        return None

    archive_path = config.ARCHIVE_DIR / f"{target_date}.tar.zst"

    # Create tar in memory, then compress with zstd
    tar_path = config.ARCHIVE_DIR / f"{target_date}.tar"

    # First create uncompressed tar
    with tarfile.open(str(tar_path), "w") as tar:
        for f in sorted(day_dir.iterdir()):
            if f.suffix == ".parquet":
                tar.add(str(f), arcname=f.name)

    # Then compress with zstd
    cctx = zstd.ZstdCompressor(level=3, threads=-1)  # Use all cores
    with open(str(tar_path), "rb") as f_in:
        with open(str(archive_path), "wb") as f_out:
            cctx.copy_stream(f_in, f_out)

    # Remove intermediate tar
    tar_path.unlink()

    orig_size = sum(f.stat().st_size for f in day_dir.iterdir() if f.suffix == ".parquet")
    archive_size = archive_path.stat().st_size
    ratio = archive_size / orig_size * 100 if orig_size > 0 else 0

    logger.info(
        f"Compressed {target_date}: "
        f"{orig_size / 1024 / 1024:.1f} MB → "
        f"{archive_size / 1024 / 1024:.1f} MB "
        f"({ratio:.1f}%)"
    )
    return archive_path

def cleanup_old_data():
    cutoff = date.today() - timedelta(days=config.UNCOMPRESSED_RETENTION_DAYS)
    removed = 0

    for day_dir in sorted(config.TICK_DIR.iterdir()):
        if not day_dir.is_dir():
            continue
        try:
            dir_date = date.fromisoformat(day_dir.name)
        except ValueError:
            continue

        if dir_date < cutoff:
            # Verify archive exists before deleting
            archive = config.ARCHIVE_DIR / f"{dir_date}.tar.zst"
            if archive.exists():
                shutil.rmtree(day_dir)
                logger.info(f"Removed old uncompressed data: {day_dir}")
                removed += 1
            else:
                logger.warning(
                    f"Skipping cleanup of {day_dir} — "
                    f"archive {archive} not found!"
                )

    if removed:
        logger.info(f"Cleaned up {removed} old directories")

def disk_usage_report() -> str:
    lines = ["", "── Disk Usage ──"]

    # Ticks (uncompressed)
    tick_size = 0
    tick_days = 0
    for day_dir in config.TICK_DIR.iterdir():
        if day_dir.is_dir():
            tick_days += 1
            for f in day_dir.iterdir():
                tick_size += f.stat().st_size
    lines.append(
        f"  Ticks (uncompressed): {tick_size / 1024 / 1024:.1f} MB "
        f"({tick_days} days)"
    )

    # Archives
    archive_size = 0
    archive_count = 0
    for f in config.ARCHIVE_DIR.iterdir():
        if f.suffix == ".zst":
            archive_size += f.stat().st_size
            archive_count += 1
    lines.append(
        f"  Archives:             {archive_size / 1024 / 1024:.1f} MB "
        f"({archive_count} days)"
    )

    # Instruments
    inst_size = sum(
        f.stat().st_size for f in config.INSTRUMENT_DIR.iterdir()
        if f.is_file()
    )
    lines.append(f"  Instruments:          {inst_size / 1024 / 1024:.1f} MB")

    # Constituents
    const_size = 0
    for root, dirs, files in os.walk(config.CONSTITUENT_DIR):
        for f in files:
            const_size += Path(root, f).stat().st_size
    lines.append(f"  Constituents:         {const_size / 1024 / 1024:.1f} MB")

    # Total
    total = tick_size + archive_size + inst_size + const_size
    lines.append(f"  ─────────────────────────────")
    lines.append(f"  Total:                {total / 1024 / 1024:.1f} MB")

    # Disk free
    stat = os.statvfs(str(config.DATA_DIR))
    free_gb = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)
    total_gb = (stat.f_blocks * stat.f_frsize) / (1024 ** 3)
    lines.append(f"  Disk free:            {free_gb:.1f} GB / {total_gb:.1f} GB")

    report = "\n".join(lines)
    logger.info(report)
    return report

def run_postmarket(storage):
    today = date.today()
    logger.info("Starting post-market processing...")

    # storage.consolidate()

    compress_day(today)

    cleanup_old_data()

    disk_usage_report()

    logger.info("Post-market processing complete")
