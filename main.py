import logging
from typing import Optional
from pathlib import Path

import typer


DEFAULT_CONFIG_PATH = str(Path('expunge', 'configs', 'default.yaml'))

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False)


@app.command()
def build_db():
    from db.build import create_all_tables
    create_all_tables()


@app.command()
def ingest(clear_existing: bool = True):
    from ingest.run import run_ingestion
    run_ingestion(clear_existing)


@app.command()
def classify(
    config_path: str = DEFAULT_CONFIG_PATH,
    write_features: bool = False,
    n_partitions: Optional[int] = None
):
    from expunge.run import run_classification
    run_classification(
        config_path,
        write_features,
        n_partitions
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s - %(module)s.py]: %(message)s',
        datefmt='%H:%M:%S'
    )

    app()
