import logging
from typing import Optional
from pathlib import Path

import typer


DEFAULT_CONFIG_PATH = str(Path('expunge', 'configs', 'default.yaml'))

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="CLI for working with expungement classification pipelines",
    add_completion=False
)


@app.command()
def build_db():
    """Build all tables for expungement classification data model"""
    from db.build import create_all_tables
    create_all_tables()


@app.command()
def ingest(
    clear_existing: bool = typer.Option(
        default=True, 
        help="Whether to delete any existing court data in DB before loading new data"
    )
):
    """Run ingestion pipeline to clean and load Virginia court data to DB"""
    from ingest.run import run_ingestion
    run_ingestion(clear_existing)


@app.command()
def classify(
    config_path: str = typer.Option(
        default=DEFAULT_CONFIG_PATH, 
        help="Path to classification run configuration YAML file"
    ),
    write_features: bool = typer.Option(
        default=False, 
        help="Whether to save classification features to DB"
    ),
    n_partitions: Optional[int] = typer.Option(
        default=None, 
        help=(
            "Number of partitions to use for Dask dataframe."
            "Pass 'None' to have Dask estimate the optimal number"
        )
    )
):
    """Run expungement classification pipeline"""
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
