import yaml
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]

with open(ROOT_DIR/"config/config.yml", "r") as f:
    conf = yaml.safe_load(f)

raw_path = conf["files"]["raw_path"]
wip_path = conf["files"]["wip_path"]
processed_path = conf["files"]["processed_path"]