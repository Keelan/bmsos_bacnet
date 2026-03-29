"""python -m edge_agent"""

from __future__ import annotations

from edge_agent.main import run
from edge_agent.settings import Settings


def main() -> None:
    run(Settings())


if __name__ == "__main__":
    main()
