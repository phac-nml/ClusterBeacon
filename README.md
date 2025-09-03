# OutbreakBeacon

**OutbreakBeacon** integrates **genomic** and **epidemiologic** data for **cluster detection** and **standardized outbreak code assignment**.

## ✨ Key Features
- **Cluster Detection** – combine WGS/cgMLST with epi linkages.
- **Outbreak Code Assignment** – stable, shareable codes for clusters.
- **Integration‑Friendly** – designed to interoperate with LIMS/surveillance tools.
- **Public‑Health Focused** – approachable for epidemiologists and analysts.

## 🚀 Quickstart
```bash
# From the repo root
pip install -e .
outbreakbeacon --help
```

Example (placeholder):
```bash
outbreakbeacon run --input data/example_dataset.tsv --output results/
```

## 📂 Structure
```
src/outbreakbeacon/   # library + CLI
tests/                # unit tests (pytest)
docs/                 # documentation stubs
data/                 # example data
.github/workflows/    # CI
```

## 🤝 Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md).

## 📜 License
MIT — see [LICENSE](LICENSE).

## 🔗 Citation
> OutbreakBeacon: Integrating genomic and epidemiologic data for cluster detection and outbreak code assignment. GitHub, 2025.
