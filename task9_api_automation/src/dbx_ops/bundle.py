import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class OrdersBundle:
    file_path: str
    catalog: str
    bronze_schema: str
    landing_volume: str
    pipeline_id: str
    jobs: dict[str, int]

    @property
    def landing_notebook(self) -> str:
        return f"{self.file_path}/notebooks/00_generate_landing_data"

    def landing_params(self) -> dict[str, str]:
        return {
            "catalog": self.catalog,
            "bronze_schema": self.bronze_schema,
            "landing_volume": self.landing_volume,
            "src_path": f"{self.file_path}/src",
        }


def load_bundle(path: str | Path) -> OrdersBundle:
    summary = json.loads(Path(path).read_text(encoding="utf-8"))
    resources = summary["resources"]
    return OrdersBundle(
        file_path=summary["workspace"]["file_path"],
        catalog=summary["variables"]["catalog"]["value"],
        bronze_schema=resources["schemas"]["bronze"]["name"],
        landing_volume=resources["volumes"]["landing"]["name"],
        pipeline_id=resources["pipelines"]["orders_pipeline"]["id"],
        jobs={key: int(job["id"]) for key, job in resources["jobs"].items()},
    )
