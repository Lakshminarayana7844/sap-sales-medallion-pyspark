"""Render SVG charts from the gold sample CSVs (committed so README shows results)."""
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def charts(sample_dir: str, img_dir: str) -> None:
    os.makedirs(img_dir, exist_ok=True)
    g = pd.read_csv(f"{sample_dir}/gold.csv")
    piv = g.pivot(index="order_month", columns="region", values="revenue").fillna(0)
    ax = piv.plot(kind="bar", stacked=True, figsize=(9, 4.5))
    ax.set_title("Monthly revenue by region (gold layer)")
    ax.set_xlabel("")
    ax.set_ylabel("Revenue")
    plt.tight_layout()
    plt.savefig(f"{img_dir}/revenue_by_region.svg", metadata={"Date": None})
    plt.close()
    m = pd.read_csv(f"{sample_dir}/gold_materials.csv")
    ax = m.plot(kind="barh", x="material", y="revenue", legend=False, figsize=(6, 3))
    ax.set_title("Revenue by material")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f"{img_dir}/revenue_by_material.svg", metadata={"Date": None})
    plt.close()


if __name__ == "__main__":
    charts(sys.argv[1] if len(sys.argv) > 1 else "data/samples", sys.argv[2] if len(sys.argv) > 2 else "docs")
