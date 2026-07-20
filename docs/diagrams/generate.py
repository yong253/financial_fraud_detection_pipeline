"""Generate the pipeline architecture diagrams (docs/diagrams/*.png).

Style: white background, thin dark "lane" boxes (no pastel cluster fills),
color reserved for tool logos and the Bronze/Silver/Gold cards — modeled
after a reference architecture diagram the user provided.

Requires:
    pip install diagrams
    Graphviz (`dot`) on PATH — winget install Graphviz.Graphviz (Windows)
      / brew install graphviz (macOS) / apt install graphviz (Linux)

Run:
    python docs/diagrams/generate.py

Outputs (next to this script):
    architecture.png  — end-to-end pipeline architecture

Windows note: if node icons render as blank boxes, Graphviz's `dot.exe` is
failing to open the icon PNGs because the Python install path contains
non-ASCII characters (e.g. a Korean-named user profile) — `dot` raises
"Illegal byte sequence" for such paths. Fix: run this script with a Python
whose full path (venv included) is pure ASCII, e.g. a venv created inside
this repo (`py -m venv .venv`, ASCII repo path).

architecture.png layout note: uses the `neato` engine with every node pinned
via pos="x,y!" (see _pos()) instead of `dot`'s automatic rank layout. This
was needed to put Data Lake below Processing Layer with a clean upward
Bronze->Spark arrow — `dot` always ranks the upstream node (Bronze) at or
before its downstream reader (Spark), which forced Data Lake above/beside
Processing regardless of declaration order. Also: neato does not draw
borders for *nested* clusters (a Cluster inside a Cluster), so Task
Orchestration / Data Processing are top-level clusters with "Processing
Layer" as a floating plaintext label (not a wrapping box) above them.
If you resize/add nodes, coordinates need to be adjusted by hand — check
the rendered PNG for overlap and nudge the relevant _pos(x, y) calls.
"""

import os

from diagrams import Cluster, Diagram, Edge, Node
from diagrams.gcp.analytics import BigQuery, Dataproc
from diagrams.gcp.storage import GCS
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.client import Client
from diagrams.onprem.monitoring import Grafana, Prometheus
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow
from diagrams.generic.storage import Storage
from diagrams.programming.language import Python

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

FONT = "Helvetica,Arial,sans-serif"
INK = "#1A1A1A"

GRAPH_ATTR = {
    "fontsize": "22",
    "fontname": FONT,
    "bgcolor": "white",
    "pad": "0.6",
    "nodesep": "0.55",
    "ranksep": "0.85",
    "splines": "spline",
}
NODE_ATTR = {
    "fontsize": "13",
    "fontname": FONT,
}
EDGE_ATTR = {
    "fontsize": "11",
    "fontname": FONT,
    "color": "#8A97A3",
}

# "Lane" cluster: white background, thin dark border, bold-ish label — no pastel fill.
LANE = {"bgcolor": "white", "pencolor": INK, "penwidth": "1.3", "fontsize": "15", "fontname": FONT, "fontcolor": INK}
LANE_DASHED = {**LANE, "style": "dashed"}

# Color reserved for the medallion cards only (small filled chips, not whole lanes).
BRONZE_CARD = dict(shape="box", style="filled,rounded", fillcolor="#C98A3E", color="#8A5A22",
                    fontname=FONT, fontsize="13", fontcolor="white", fixedsize="false",
                    width="1.3", height="0.6", margin="0.15,0.08")
SILVER_CARD = dict(shape="box", style="filled,rounded", fillcolor="#B7BEC6", color="#7A828A",
                    fontname=FONT, fontsize="13", fontcolor="#20242A", fixedsize="false",
                    width="1.3", height="0.6", margin="0.15,0.08")
GOLD_CARD = dict(shape="box", style="filled,rounded", fillcolor="#EFC94C", color="#A9862A",
                  fontname=FONT, fontsize="13", fontcolor="#4A3B0A", fixedsize="false",
                  width="1.3", height="0.6", margin="0.15,0.08")

def _pos(x: float, y: float) -> str:
    """Pinned neato position (points, origin bottom-left, y up)."""
    return f"{x},{y}!"


def build_architecture() -> None:
    neato_graph_attr = {
        **GRAPH_ATTR,
        "overlap": "false",
        "splines": "curved",
        "sep": "+4",
        "pad": "0.4",
    }
    with Diagram(
        "Financial Fraud Detection Pipeline",
        filename=os.path.join(OUT_DIR, "architecture"),
        show=False,
        graph_attr=neato_graph_attr,
        node_attr=NODE_ATTR,
        edge_attr=EDGE_ATTR,
        outformat="png",
    ) as diag:
        diag.dot.engine = "neato"

        with Cluster("Ingestion (always-on)", graph_attr=LANE_DASHED):
            csv = Storage("PaySim CSV", pos=_pos(60, 500))
            producer = Python("Producer", pos=_pos(170, 500))
            kafka = Kafka("Kafka", pos=_pos(280, 500))
            csv >> producer >> kafka

        with Cluster("Data Lake", graph_attr=LANE):
            bronze = Node("Bronze", pos=_pos(400, 150), **BRONZE_CARD)
            silver = Node("Silver", pos=_pos(530, 150), **SILVER_CARD)
            gold = Node("Gold", pos=_pos(660, 150), **GOLD_CARD)
            gcs = GCS("GCS", pos=_pos(790, 150))

        # Outer wrapper box — mirrors the reference image's single unlabeled big
        # box that holds BOTH "Processing Layer" and "Serving Layer" (the two are
        # just floating text labels over one shared box, not two separate boxes).
        # neato doesn't draw borders for *nested* clusters, so this wrapper, Task
        # Orchestration, Data Processing and Monitoring are all top-level clusters
        # whose corner anchors happen to nest visually. The wrapper now spans the
        # combined width of Data Processing + Monitoring (previously it stopped
        # short of Monitoring, leaving it floating outside — the bug being fixed).
        with Cluster("", graph_attr=LANE):
            corner_tl = Node("", pos=_pos(355, 585), shape="point", style="invis", width="0.01")
            corner_br = Node("", pos=_pos(1160, 275), shape="point", style="invis", width="0.01")

        # Floating layer titles (text only, no box), positioned above each half of
        # the wrapper — exactly like the reference's "Processing Layer" / "Serving
        # Layer" labels sitting over one shared box.
        processing_label = Node(
            "Processing Layer", pos=_pos(530, 610), shape="plaintext",
            fontsize="16", fontname=FONT, fontcolor=INK,
        )
        serving_label = Node(
            "Serving Layer", pos=_pos(980, 610), shape="plaintext",
            fontsize="16", fontname=FONT, fontcolor=INK,
        )

        # Task Orchestration spans the full width of Data Processing + Monitoring
        # combined (reference: Airflow bar stretches across the whole bottom row).
        with Cluster("Task Orchestration", graph_attr=LANE):
            anchor_l = Node("", pos=_pos(400, 495), shape="point", style="invis", width="0.01")
            airflow = Airflow("Airflow", pos=_pos(755, 495))
            anchor_r = Node("", pos=_pos(1110, 495), shape="point", style="invis", width="0.01")

        with Cluster("Data Processing", graph_attr=LANE):
            spark = Dataproc("Spark\n(Dataproc)", pos=_pos(400, 330))
            bigquery = BigQuery("BigQuery", pos=_pos(530, 330))
            dbt = Dbt("dbt", pos=_pos(660, 330))
            bigquery >> dbt

        with Cluster("Monitoring", graph_attr=LANE):
            pushgw = Prometheus("Pushgateway", pos=_pos(850, 330))
            prom = Prometheus("Prometheus", pos=_pos(980, 330))
            graf = Grafana("Grafana", pos=_pos(1110, 330))
            pushgw >> prom >> graf

        analyst = Client("Analyst", pos=_pos(1300, 330))

        kafka >> Edge(label="Kafka Connect") >> bronze
        bronze >> Edge(label="read") >> spark
        spark >> Edge(label="write") >> silver
        silver >> Edge(label="read") >> bigquery
        dbt >> Edge(label="write") >> gold
        gold >> Edge(style="dashed", color="#B3403A", fontcolor="#B3403A", label="reconcile") >> silver
        gold >> pushgw
        graf >> analyst


if __name__ == "__main__":
    build_architecture()
    print("Generated:")
    print(f"  {os.path.join(OUT_DIR, 'architecture.png')}")
