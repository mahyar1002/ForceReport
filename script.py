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


def parse_table(table_lines, dtype=None):
    table_str = "\n".join(table_lines)
    if dtype is not None:
        df = pd.read_csv(StringIO(table_str), header=None, dtype=dtype)
    else:
        df = pd.read_csv(StringIO(table_str), header=None)
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    return df


def get_intersection(set_1, set_2):
    common = set_1 & set_2
    if common and len(common) == 1:
        return next(iter(common))
    return None


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
                property_id = beam_info.iloc[0]['property_id']  # Get first property_id
                
                # Step 3: Look up property_name for this property_id in sections_df
                section_info = sections_df[sections_df['property_id'] == property_id]
                if not section_info.empty:
                    property_name = section_info.iloc[0]['name']  # Get first property_name
                    
                    # Add to reaction_df
                    reaction_df.at[idx, 'property_id'] = property_id
                    reaction_df.at[idx, 'property_name'] = property_name
                else:
                    print(f"{property_id} not found in section_df for node {node} and beam_id {beam_id}")
            else:
                print(f"{beam_id} not found in beams_df for node {node}")
        else:
            print(f"{node} not found in beam_end_forces_df")

    return reaction_df


def run(input_path, class_1, class_2, lc):
    # Load CSV lines, skipping the first 15 lines
    with open(input_path, encoding="iso-8859-1") as f:
        lines = f.readlines()[15:]

    # Extract tables
    beams_lines = extract_table(lines, "Beams", ["Sections"])
    sections_lines = extract_table(lines, "Sections", ["Supports"])
    reaction_lines = extract_table(
        lines, "Reactions", ["Beam End Forces"])
    beam_end_forces_lines = extract_table(
        lines, "Beam End Forces", ["18 May", "STAAD.Pro"])

    # Parse to DataFrames
    print("Parsing sections...")
    sections_df = parse_table(sections_lines)
    sections_df = sections_df.iloc[3:, 1:].reset_index(drop=True)
    sections_df.columns = ["property_id", "name",
                           "area", "iyy", "izz", "j", "material", "source"]
    
    print("Parsing beams...")
    beams_df = parse_table(beams_lines)
    beams_df = beams_df.iloc[3:, 1:].reset_index(drop=True)
    beams_df.columns = ["beam_id", "node_a",
                        "node_b", "len", "property_id", "beta"]
    beams_df['beam_id'] = beams_df['beam_id'].astype(int)
    beams_df['node_a'] = beams_df['node_a'].astype(int)
    beams_df['node_b'] = beams_df['node_b'].astype(int)

    print("Parsing forces...")
    beam_end_forces_df = parse_table(beam_end_forces_lines)
    beam_end_forces_df = beam_end_forces_df.iloc[4:, 1:].reset_index(drop=True)
    beam_end_forces_df.columns = [
        "beam_id", "node", "lc", "fx", "fy", "fz", "mx", "my", "mz"]
    beam_end_forces_df['beam_id'] = beam_end_forces_df['beam_id'].ffill()
    beam_end_forces_df['beam_id'] = beam_end_forces_df['beam_id'].astype(int)
    beam_end_forces_df['node'] = beam_end_forces_df['node'].astype(int)

    print("Parsing reactions...")
    reaction_df = parse_table(reaction_lines)
    reaction_df = reaction_df.iloc[4:, 1:].reset_index(drop=True)
    reaction_df.columns = ["node", "lc", "fx", "fy", "fz", "mx", "my", "mz"]
    reaction_df['node'] = reaction_df['node'].ffill()
    reaction_df['node'] = reaction_df['node'].astype(int)
    reaction_df = update_reaction_table(
        sections_df, beams_df, beam_end_forces_df, reaction_df)

    filtered_sections_class_1_df = sections_df[sections_df["name"] == class_1]
    filtered_sections_class_2_df = sections_df[sections_df["name"] == class_2]

    filtered_beams_class_1_df = beams_df[beams_df["property_id"].isin(
        filtered_sections_class_1_df["property_id"])]
    filtered_beams_class_2_df = beams_df[beams_df["property_id"].isin(
        filtered_sections_class_2_df["property_id"])]


    # Step 1: Create the Cartesian product (cross join)
    cross_df = filtered_beams_class_1_df.assign(key=1).merge(
        filtered_beams_class_2_df.assign(key=1),
        on="key",
        suffixes=('_1', '_2')
    ).drop(columns=["key"])

    # Step 2: Remove rows with same beam_id
    cross_df = cross_df[cross_df["beam_id_1"] != cross_df["beam_id_2"]]

    # Step 3: Apply the intersection logic
    def get_common_node(row):
        nodes_1 = {row["node_a_1"], row["node_b_1"]}
        nodes_2 = {row["node_a_2"], row["node_b_2"]}
        common = nodes_1 & nodes_2
        return common.pop() if common else None

    cross_df["node"] = cross_df.apply(get_common_node, axis=1)

    # Step 4: Filter rows where a common node was found
    intersection_beams_df = cross_df[cross_df["node"].notnull()]
    intersection_beams_df = intersection_beams_df.drop_duplicates()
    intersection_beams_df = intersection_beams_df.drop_duplicates()

    # Step 5: filter force dataframe based on intersection beams
    filtered_forces_df = beam_end_forces_df[
        (beam_end_forces_df["lc"].isin(lc)) &
        (
            beam_end_forces_df[["node", "beam_id"]]
            .apply(tuple, axis=1)
            .isin(
                pd.concat([
                    intersection_beams_df[["node", "beam_id_1"]].rename(
                        columns={"beam_id_1": "beam_id"}),
                    intersection_beams_df[["node", "beam_id_2"]].rename(
                        columns={"beam_id_2": "beam_id"})
                ])
                .apply(tuple, axis=1)
            )
        )
    ]

    with pd.ExcelWriter("report.xlsx", engine='xlsxwriter') as writer:
        filtered_forces_df.to_excel(
            writer, sheet_name="final force report", index=False)
        sections_df.to_excel(
            writer, sheet_name="extracted sections", index=False)
        beams_df.to_excel(writer, sheet_name="extracted beams", index=False)
        beam_end_forces_df.to_excel(
            writer, sheet_name="extracted forces", index=False)
        reaction_df.to_excel(
            writer, sheet_name="extracted reactions", index=False)


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
