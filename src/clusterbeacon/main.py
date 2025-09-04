from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter)
from src.clusterbeacon.version import __version__
from src.clusterbeacon.classes.detector import detector
import json
import os
import sys
from datetime import datetime
from pathlib import Path


def parse_args():
    class CustomFormatter(ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter):
        pass

    parser = ArgumentParser(
        description=(
            "Cluster Beacon: rule-based recognition of outbreak events using metadata "
            f"and genetic relationships (v{__version__})"
        ),
        formatter_class=CustomFormatter,
    )
    parser.add_argument(
        "--ll",
        "-i",
        dest="line_list",
        type=Path,
        required=False,
        help="Arborator line list (TSV) output path",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        required=True,
        help="Configuration file (YAML or JSON)",
    )
    parser.add_argument(
        "--outdir",
        "-o",
        type=Path,
        required=False,
        help="Output directory for result files (overrides config)",
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Overwrite existing output directory if it exists",
    )
    parser.add_argument(
        "-V", "--version", action="version", version="%(prog)s " + __version__
    )

    return parser.parse_args()


def prepare_outdir(outdir: Path, force: bool) -> None:
    if outdir.exists():
        if not outdir.is_dir():
            print(
                f"Error: output path exists and is not a directory: {outdir}",
                file=sys.stderr,
            )
            sys.exit(1)
        if not force:
            print(
                f"Error: directory '{outdir}' already exists; use --force to overwrite.",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        outdir.mkdir(parents=True, exist_ok=True)




def run_outbreak_detector(config):
    config['analysis_start_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    outdir = config['outdir']
    if not os.path.isdir(outdir):
        os.makedirs(outdir, 0o755)
    elif not config['force']:
        print(f'Error directory {outdir} already exists but force not specified')
        sys.exit()
    
    obj = detector(config=config)
    status = obj.status
    if not status:
        print(f'Error something went wrong please check the log messages: \n {obj.messages}')
        sys.exit()

    obj.outbreak_df.to_csv(os.path.join(outdir,"outbreak_summary.tsv"),sep="\t",header=True, index=False)
    obj.ll_df.to_csv(os.path.join(outdir,"line_list.tsv"),sep="\t",header=True, index=False)
    fh = open(os.path.join(outdir,"duplicates.tsv"),'w')
    for md5 in obj.duplicate_candidates:
        for record in obj.duplicate_candidates[md5]:
            data = "\t".join([str(x) for x in record])
            fh.write(f'{md5}\t{data}\n')
    fh.close()

    config['analysis_end_time'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    #write run parameters
    with open(os.path.join(outdir,"run.json"),'w' ) as fh:
        fh.write(json.dumps(config, indent=4))


# ----------------------------
# Entrypoint
# ----------------------------
def main() -> None:
    args = parse_args()

    # Load config (YAML preferred, JSON supported)
    config = _load_config(args.config)

    # CLI overrides
    if args.line_list:
        config["line_list_path"] = str(args.line_list)
    if args.outdir:
        config["outdir"] = str(args.outdir)
    else:
        # Ensure a default exists if not provided in config
        config.setdefault("outdir", "results")

    # Force flag overrides config
    config["force"] = bool(args.force)

    run_outbreak_detector(config)


if __name__ == "__main__":
    main()