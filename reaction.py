import pandas as pd
import argparse
import ast
from io import StringIO
import os
from pathlib import Path


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


def create_dataframes(input_path, lc):
    # Load CSV lines, skipping the first 30 lines
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
    reaction_df['fx'] = reaction_df['fx'].astype(float)
    reaction_df['fy'] = reaction_df['fy'].astype(float)
    reaction_df['fz'] = reaction_df['fz'].astype(float)

    reaction_df = update_reaction_table(
        sections_df, beams_df, beam_end_forces_df, reaction_df)

    reaction_df = reaction_df[reaction_df["lc"].isin(lc)]
    reaction_df = reaction_df.sort_values(by='property_name', ascending=False)

    return reaction_df


def compare_reactions(reaction_df_1, reaction_df_2):
    df_1_result = reaction_df_1.copy()
    df_2_result = reaction_df_2.copy()

    # For df_1: merge with df_2 to get corresponding values
    df_1_merged = df_1_result.merge(
        reaction_df_2[['node', 'lc', 'fx', 'fy', 'fz']],
        on=['node', 'lc'],
        how='left',
        suffixes=('', '_df2')
    )

    # Calculate differences for df_1
    df_1_merged['fx1-fx2'] = df_1_merged['fx'] - df_1_merged['fx_df2']
    df_1_merged['fy1-fy2'] = df_1_merged['fy'] - df_1_merged['fy_df2']
    df_1_merged['fz1-fz2'] = df_1_merged['fz'] - df_1_merged['fz_df2']

    # Remove temporary columns and keep only original columns + difference columns
    df_1_result = df_1_merged.drop(['fx_df2', 'fy_df2', 'fz_df2'], axis=1)

    # For df_2: merge with df_1 to get corresponding values
    df_2_merged = df_2_result.merge(
        reaction_df_1[['node', 'lc', 'fx', 'fy', 'fz']],
        on=['node', 'lc'],
        how='left',
        suffixes=('', '_df1')
    )

    # Calculate differences for df_2
    df_2_merged['fx2-fx1'] = df_2_merged['fx'] - df_2_merged['fx_df1']
    df_2_merged['fy2-fy1'] = df_2_merged['fy'] - df_2_merged['fy_df1']
    df_2_merged['fz2-fz1'] = df_2_merged['fz'] - df_2_merged['fz_df1']

    # Remove temporary columns
    df_2_result = df_2_merged.drop(['fx_df1', 'fy_df1', 'fz_df1'], axis=1)

    return df_1_result, df_2_result


def run(input_path_1, input_path_2, lc):
    reaction_df_1 = create_dataframes(input_path_1, lc)
    reaction_df_2 = create_dataframes(input_path_2, lc)
    reaction_df_1, reaction_df_2 = compare_reactions(
        reaction_df_1, reaction_df_2)

    with pd.ExcelWriter("reaction_report.xlsx", engine='xlsxwriter') as writer:
        reaction_df_1.to_excel(
            writer, sheet_name=Path(input_path_1).stem, index=False)
        reaction_df_2.to_excel(
            writer, sheet_name=Path(input_path_2).stem, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path_1", required=True,
                        type=str, help="Path of the first input file")
    parser.add_argument("--input_path_2", required=True,
                        type=str, help="Path of the second input file")
    parser.add_argument("--lc", required=True, type=str,
                        help="List of load cases, e.g. '[1, 2, 203]'")
    args = parser.parse_args()
    input_path_1 = args.input_path_1
    input_path_2 = args.input_path_2
    lc = [str(item) for item in ast.literal_eval(args.lc)] if args.lc else None
    run(input_path_1, input_path_2, lc)
