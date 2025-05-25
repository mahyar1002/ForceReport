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


def update_reaction_table(sections_df, beams_df, beam_end_forces_df, reaction_df):
    reaction_df['property_id'] = None
    reaction_df['property_name'] = None

    # Iterate through each row of reaction_df
    for idx, row in reaction_df.iterrows():
        node = row['node']

        # Step 1: Look up first beam_id for this node in beam_end_forces_df
        beam_row = beam_end_forces_df[beam_end_forces_df['node'] == node]
        if not beam_row.empty:
            beam_id = beam_row.iloc[0]['beam_id']  # Get first beam_id

            # Step 2: Look up property_id for this beam_id in beams_df
            beam_info = beams_df[beams_df['beam_id'] == beam_id]
            if not beam_info.empty:
                # Get first property_id
                property_id = beam_info.iloc[0]['property_id']

                # Step 3: Look up property_name for this property_id in sections_df
                section_info = sections_df[sections_df['property_id']
                                           == property_id]
                if not section_info.empty:
                    # Get first property_name
                    property_name = section_info.iloc[0]['name']

                    # Add to reaction_df
                    reaction_df.at[idx, 'property_id'] = property_id
                    reaction_df.at[idx, 'property_name'] = property_name
                else:
                    print(
                        f"{property_id} not found in section_df for node {node} and beam_id {beam_id}")
            else:
                print(f"{beam_id} not found in beams_df for node {node}")
        else:
            print(f"{node} not found in beam_end_forces_df")

    return reaction_df


def parse_table(table_lines):
    table_str = "\n".join(table_lines)
    df = pd.read_csv(StringIO(table_str), header=None)
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    return df


def run(input_path, class_1, class_2, lc):
    # Load CSV lines, skipping the first 15 lines
    with open(input_path, encoding="iso-8859-1") as f:
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
    parser.add_argument("--input_path", required=True,
                        type=str, help="Path of the input file")
    parser.add_argument("--class_1", required=True,
                        type=str, help="First section name")
    parser.add_argument("--class_2", required=True,
                        type=str, help="Second section name")
    parser.add_argument("--lc", required=True, type=str,
                        help="List of load cases, e.g. '[1, 2, 203]'")
    args = parser.parse_args()
    input_path = args.input_path
    class_1 = args.class_1
    class_2 = args.class_2
    lc = [str(item) for item in ast.literal_eval(args.lc)]
    run(input_path, class_1, class_2, lc)
