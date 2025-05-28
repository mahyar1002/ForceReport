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
        df = pd.read_csv(StringIO(table_str), header=None, low_memory=False)
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    return df


def get_intersection(set_1, set_2):
    common = set_1 & set_2
    if common and len(common) == 1:
        return next(iter(common))
    return None


def update_reaction_table(sections_df, beams_df, beam_end_forces_df, reaction_df):
    # Step 1: Get the first beam_id for each node from beam_end_forces_df
    node_to_beam = beam_end_forces_df.drop_duplicates(
        subset=['node'], keep='first')[['node', 'beam_id']]

    # Step 2: Merge reaction_df with node_to_beam to get beam_id
    reaction_df = reaction_df.merge(node_to_beam, on='node', how='left')

    # Step 3: Merge with beams_df to get property_id
    reaction_df = reaction_df.merge(
        beams_df[['beam_id', 'property_id']], on='beam_id', how='left')

    # Step 4: Merge with sections_df to get property_name
    reaction_df = reaction_df.merge(
        sections_df[['property_id', 'name']], on='property_id', how='left')

    # Step 5: Rename the 'name' column to 'property_name'
    reaction_df = reaction_df.rename(columns={'name': 'property_name'})

    return reaction_df


def force_report(sections_df, beams_df, beam_end_forces_df, nodes_df):
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

        # Condition 1: Check if common node exists
        if not common:
            return None

        # Remove common nodes from combination of sets
        remaining_nodes = (nodes_1 | nodes_2) - common

        # Should have exactly 2 remaining nodes after removing common node(s)
        if len(remaining_nodes) != 2:
            return None

        # Convert to list to access by index
        remaining_nodes_list = list(remaining_nodes)
        node1 = remaining_nodes_list[0]
        node2 = remaining_nodes_list[1]

        # Condition 2: Check if these two nodes have at least two common values on their axis (x,y,z)
        try:
            # Get coordinates for both nodes
            node1_info = nodes_df[nodes_df['node'] == node1].iloc[0]
            node2_info = nodes_df[nodes_df['node'] == node2].iloc[0]

            # Check how many coordinates are the same
            common_coordinates = 0
            offset = 0.1
            if abs(float(node1_info['x']) - float(node2_info['x'])) <= offset:
                common_coordinates += 1
            if abs(float(node1_info['y']) - float(node2_info['y'])) <= offset:
                common_coordinates += 1
            if abs(float(node1_info['z']) - float(node2_info['z'])) <= offset:
                common_coordinates += 1

            # If at least two coordinates are common, return None
            if common_coordinates >= 2:
                return None
            else:
                # Otherwise return the common node
                return common.pop()

        except (IndexError, KeyError):
            # If nodes not found in nodes_df, return None
            return None

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

    return filtered_forces_df


def run(input_path, class_1, class_2, lc):
    # Load CSV lines, skipping the first 15 lines
    with open(input_path, encoding="iso-8859-1") as f:
        lines = f.readlines()[30:]

    # Extract tables
    nodes_lines = extract_table(lines, "Nodes", ["Beams"])
    beams_lines = extract_table(lines, "Beams", ["Supports", "Sections"])
    sections_lines = extract_table(
        lines, "Sections", ["Supports",  "STAAD.Pro"])
    reaction_lines = extract_table(
        lines, "Reactions", ["Beam End Forces"])
    beam_end_forces_lines = extract_table(
        lines, "Beam End Forces", ["Max Forces by Property"])

    # Parse to DataFrames
    print("Parsing nodes...")
    nodes_df = parse_table(nodes_lines)
    nodes_df = nodes_df.iloc[3:, 1:].reset_index(drop=True)
    nodes_df.columns = ["node", "x", "y", "z"]
    nodes_df['node'] = nodes_df['node'].astype(int)

    print("Parsing sections...")
    sections_df = parse_table(sections_lines)
    sections_df = sections_df.iloc[3:, 1:].reset_index(drop=True)
    sections_df.columns = ["property_id", "name",
                           "area", "iyy", "izz", "j", "material", "source"]
    sections_df['property_id'] = sections_df['property_id'].astype(int)

    print("Parsing beams...")
    beams_df = parse_table(beams_lines)
    beams_df = beams_df.iloc[3:, 1:].reset_index(drop=True)
    beams_df.columns = ["beam_id", "node_a",
                        "node_b", "len", "property_id", "beta"]
    beams_df['beam_id'] = beams_df['beam_id'].astype(int)
    beams_df['node_a'] = beams_df['node_a'].astype(int)
    beams_df['node_b'] = beams_df['node_b'].astype(int)
    beams_df['property_id'] = beams_df['property_id'].astype(int)

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
    reaction_df = reaction_df[reaction_df["lc"].isin(lc)]

    filtered_forces_df = force_report(
        sections_df, beams_df, beam_end_forces_df, nodes_df)

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
        nodes_df.to_excel(
            writer, sheet_name="extracted nodes", index=False)


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
