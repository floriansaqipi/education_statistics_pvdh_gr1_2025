import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def phase1_path(*parts):
    return os.path.join(ROOT, "data", "phase_1", "output", *parts)


def phase2_path(*parts):
    return os.path.join(ROOT, "data", "phase_2", "output", *parts)


def data_path(*parts):
    return os.path.join(ROOT, "data", *parts)
