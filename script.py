import pandas as pd

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
    from io import StringIO
    table_str = "\n".join(table_lines)
    df = pd.read_csv(StringIO(table_str), header=None)
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    return df

# Load CSV lines, skipping the first 15 lines
with open("test01.csv", encoding="utf-8") as f:
    lines = f.readlines()[15:]

# Extract tables
beams_lines = extract_table(lines, "Beams", ["Sections"])
sections_lines = extract_table(lines, "Sections", ["Supports"])
beam_end_forces_lines = extract_table(lines, "Beam End Forces", ["18 May", "STAAD.Pro"])

# Parse to DataFrames
beams_df = parse_table(beams_lines)
beams_df = beams_df.iloc[3:, 1:].reset_index(drop=True)
beams_df.columns = ["beam_id", "node_a", "node_b", "len", "property_id", "beta"]

sections_df = parse_table(sections_lines)
sections_df = sections_df.iloc[3:, 1:].reset_index(drop=True)
sections_df.columns = ["property_id", "name", "area", "iyy", "izz", "j", "material", "source"]

beam_end_forces_df = parse_table(beam_end_forces_lines)
beam_end_forces_df = beam_end_forces_df.iloc[4:, 1:].reset_index(drop=True)
beam_end_forces_df.columns = ["beam_id", "node", "lc", "fx", "fy", "fz", "mx", "my", "mz"]
beam_end_forces_df['beam_id'] = beam_end_forces_df['beam_id'].fillna(method='ffill')

# Example: Show first rows
# print("Beams:")
# print(beams_df.head())
# print("\nSections:")
# print(sections_df.head())
# print("\nBeam End Forces:")
# print(beam_end_forces_df.head())

print(beam_end_forces_df)

# Save to new CSVs
beams_df.to_csv("beams_extracted.csv", index=False)
sections_df.to_csv("sections_extracted.csv", index=False)
beam_end_forces_df.to_csv("beam_end_forces_extracted.csv", index=False)

