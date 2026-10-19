import json
import time
from dataclasses import asdict, dataclass, field


@dataclass
class Step:
    name: str
    status: str = "PENDING"
    details: str = ""
    url: str = ""
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status in ("SUCCESS", "SKIPPED")


@dataclass
class Report:
    title: str
    steps: list[Step] = field(default_factory=list)

    def start(self, name: str) -> Step:
        step = Step(name, status="RUNNING")
        step.seconds = time.monotonic()
        self.steps.append(step)
        log(f"{name}: started")
        return step

    def finish(self, step: Step, status: str, details: str = "", url: str = "") -> None:
        step.status, step.details, step.url = status, details or step.details, url or step.url
        step.seconds = round(time.monotonic() - step.seconds, 1)
        log(f"{step.name}: {status} in {step.seconds}s {details}".rstrip())

    @property
    def ok(self) -> bool:
        return all(s.ok for s in self.steps)

    def to_markdown(self) -> str:
        lines = [
            f"### {self.title}: {'SUCCESS' if self.ok else 'FAILED'}",
            "",
            "| Step | Status | Time, s | Details |",
            "|---|---|---|---|",
        ]
        for s in self.steps:
            name = f"[{s.name}]({s.url})" if s.url else s.name
            lines.append(f"| {name} | {s.status} | {s.seconds} | {s.details.replace('|', '/')} |")
        return "\n".join(lines) + "\n"

    def to_json(self) -> str:
        return json.dumps({"title": self.title, "ok": self.ok, "steps": [asdict(s) for s in self.steps]}, indent=2)


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)
