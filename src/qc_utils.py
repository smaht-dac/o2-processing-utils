import os
from src.constants import SUPPORTED_QC_TOOLS, SAMTOOLS_STATS
from pydantic import BaseModel, RootModel
from typing import List, Dict
from pathlib import Path
import csv


class QC_location(BaseModel):
    qc_tool: str
    output_path: str


class QC_locations(RootModel):
    root: List[QC_location]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]


def parse_and_store_qc_outputs(qcs: QC_locations, tsv_path: str):
    """Parses the supplied QC files and generates a TSV file with the results. It will be
    stored to path

    Args:
        qcs (QC_locations):  Defines type and location of the QCs to parse
        path (str): location of the resulting TSV file
    """

    all_metrics = parse_qc_outputs(qcs)
    sorted_keys = sorted(list(all_metrics.keys()))
    sorted_metrics = []

    for key in sorted_keys:
        sorted_metrics.append(all_metrics[key])
   
    tsv_dir = os.path.dirname(tsv_path) 
    Path("tsv_dir").mkdir(parents=True, exist_ok=True)
    
    with open(tsv_path, "w") as outfile:
        csvwriter = csv.writer(outfile, delimiter="\t")
        csvwriter.writerow(sorted_keys)
        csvwriter.writerow(sorted_metrics)


def parse_qc_outputs(qcs: QC_locations) -> Dict:
    """This function goes through the list of supplied QC files and parses their output
    It returns a dict with all combined metrics.

    Args:
        qcs (QC_locations): Defines type and location of the QCs to parse
    """
    
    metrics_combined = {}

    for qc in qcs:
        qc_tool = qc.qc_tool
        qc_output_path = qc.output_path
        if qc_tool not in SUPPORTED_QC_TOOLS:
            raise Exception(f"Tried to parse an unsupported QC tool: {qc_tool}")

        try:
            if qc_tool == SAMTOOLS_STATS:
                new_metrics = parse_samtools_stats(qc_output_path)
            metrics_combined = {**metrics_combined, **new_metrics}
        except Exception as e:
            raise Exception(f"Error parsing QC file: {str(e)}")
    
    return metrics_combined

def create_summary_qc_file(qc_folder: str, summary_qc_path):
    """This function searches for all .qc files in the given folder
    and combines the metrics into a single summary qc file.
    We assume that all qc files contain the same metrics (same header)!

    Args:
        qc_folder (str): Folder with .qc files in it
        summary_qc_path (str): File path to the summary qc file
    """

    current_keys = None
    all_values = []
    pathlist = Path(qc_folder).rglob("*.qc")
    for path_obj in pathlist:
        file_name = path_obj.name
        # because path is an object not string
        path = str(path_obj)

        with open(path) as qc_file:
            tsv_file = csv.reader(qc_file, delimiter="\t")
            keys = next(tsv_file)  # First line
            keys.insert(0, "File name")
            # Make sure that TSV headers are the same
            if current_keys and ",".join(keys) != ",".join(current_keys):
                raise Exception(
                    "Can't merge QC files. The TSV files have different headers."
                )
            current_keys = keys

            values = next(tsv_file)
            values.insert(0, file_name)
            all_values.append(values)  # Second line

    with open(summary_qc_path, "w") as outfile:
        csvwriter = csv.writer(outfile, delimiter="\t")
        csvwriter.writerow(current_keys)
        for metrics in all_values:
            csvwriter.writerow(metrics)


def parse_samtools_stats(path):
    metrics = {}
    # Parse file and save values
    with open(path) as fi:
        for line in fi:
            if line.startswith("SN"):
                line = line.rstrip().split("\t")
                field, value = line[1].replace(":", ""), line[2]
                metrics[f"{SAMTOOLS_STATS}: {field}"] = value
    return metrics
