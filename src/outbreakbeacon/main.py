from argparse import (ArgumentParser, ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter)
from outbreak_detector.version import __version__
from outbreak_detector.classes.detector import detector
import json
import os
import sys
import pandas as pd
from datetime import datetime

def parse_args():
    class CustomFormatter(ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter):
        pass

    parser = ArgumentParser(
        description="Outbreak detector: rule based recognition of outbreak events using metadata and genetic relationships v. {}".format(__version__),
        formatter_class=CustomFormatter)
    parser.add_argument('--input','-i', type=str, required=False, help='Arborator line list output')
    parser.add_argument('--outdir', '-o', type=str, required=False, help='Result output files')
    parser.add_argument('--config', '-c', type=str, required=True,
                        help='Configuration json')
    parser.add_argument('--force','-f', required=False, help='Overwrite existing directory',
                        action='store_true')
    parser.add_argument('-V', '--version', action='version', version="%(prog)s " + __version__)

    return parser.parse_args()


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

def main():
    cmd_args = parse_args()
    input_path = cmd_args.input
    outdir_path = cmd_args.outdir
    config_path = cmd_args.config
    force = cmd_args.force

    if not os.path.isfile(config_path):
        print(f'Error config file: {config_path} does not exist or is inaccessible')
        sys.exit()
    with open(config_path, 'r') as fh:
        config = json.loads(fh.read())
     
    if input_path is not None and len(input_path) > 0:
        config['line_list_path'] = input_path

    if outdir_path  is not None and len(outdir_path) > 0:
        config['outdir'] = outdir_path
    
    if force is not None:
        config['force'] = True
    else:
         config['force'] = False       
      
    run_outbreak_detector(config)


# call main function
if __name__ == '__main__':
    main()