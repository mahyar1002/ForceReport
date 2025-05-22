import pandas as pd
import argparse
import ast
from io import StringIO


def extract_table(lines, start_keyword, next_keywords):
    start = None
    end = None
    for i, line in enumerate(lines):
        if start is None and start_keyword in line:
            start = i
        elif start is not None and any(k in line for k in next_keywords):
            end = i
            break
    return lines[start:end] if start is not None else []


def parse_table(table_lines):
    table_str = "\n".join(table_lines)
    df = pd.read_csv(StringIO(table_str), header=None)
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    return df


def run(class_1, class_2, lc):
    # Load CSV lines, skipping the first 15 lines
    with open("building4testing.csv", encoding="iso-8859-1") as f:
        lines = f.readlines()[15:]

    # Extract tables
    beams_lines = extract_table(lines, "Beams", ["Sections"])
    sections_lines = extract_table(lines, "Sections", ["Supports"])
    beam_end_forces_lines = extract_table(
        lines, "Beam End Forces", ["18 May", "STAAD.Pro"])

    # Parse to DataFrames
    beams_df = parse_table(beams_lines)
    beams_df = beams_df.iloc[3:, 1:].reset_index(drop=True)
    beams_df.columns = ["beam_id", "node_a",
                        "node_b", "len", "property_id", "beta"]

    sections_df = parse_table(sections_lines)
    sections_df = sections_df.iloc[3:, 1:].reset_index(drop=True)
    sections_df.columns = ["property_id", "name",
                           "area", "iyy", "izz", "j", "material", "source"]

    beam_end_forces_df = parse_table(beam_end_forces_lines)
    beam_end_forces_df = beam_end_forces_df.iloc[4:, 1:].reset_index(drop=True)
    beam_end_forces_df.columns = [
        "beam_id", "node", "lc", "fx", "fy", "fz", "mx", "my", "mz"]
    beam_end_forces_df['beam_id'] = beam_end_forces_df['beam_id'].ffill()

    filtered_sections_df = sections_df[sections_df["name"].isin(
        [class_1, class_2])]
    filtered_beams_df = beams_df[beams_df["property_id"].isin(
        filtered_sections_df["property_id"])]
    filtered_forces_df = beam_end_forces_df[
        (beam_end_forces_df["beam_id"].isin(filtered_beams_df["beam_id"])) &
        (beam_end_forces_df["lc"].isin(lc))
    ]

    with pd.ExcelWriter("report.xlsx", engine='xlsxwriter') as writer:
        filtered_forces_df.to_excel(
            writer, sheet_name="final force report", index=False)
        sections_df.to_excel(
            writer, sheet_name="extracted sections", index=False)
        beams_df.to_excel(writer, sheet_name="extracted beams", index=False)
        beam_end_forces_df.to_excel(
            writer, sheet_name="extracted forces", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--class_1", required=True,
                        type=str, help="First section name")
    parser.add_argument("--class_2", required=True,
                        type=str, help="Second section name")
    parser.add_argument("--lc", required=True, type=str,
                        help="List of load cases, e.g. '[1, 2, 203]'")
    args = parser.parse_args()
    class_1 = args.class_1
    class_2 = args.class_2
    lc = [str(item) for item in ast.literal_eval(args.lc)]
    run(class_1, class_2, lc)
