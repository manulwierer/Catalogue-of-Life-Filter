import pandas as pd
import sys
from pathlib import Path
from collections import defaultdict

INPUT_FILE = r"yourinputpathhere\example.csv"

OUTPUT_FILE = r"youroutputpathhere\example.csv"

ID_COLUMN = "col:ID"
PARENT_ID_COLUMN = "col:parentID"

ROOT_ID = "N"

# Animalia=N
# Protozoa=Z
# Fungi=F
# Chromista=C
# Plantae=P
# Archaea=CRLT8
# Bacteria=CRRY6
# Eukaryota=CS5HF
# Virae=L2TCC

CHUNK_SIZE = 50000

def validate_paths():
    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    if not input_path.suffix.lower() == ".csv":
        print(f"Error: Input file must be a CSV file")
        sys.exit(1)
    
    if not output_path.parent.exists():
        print(f"Error: Output directory does not exist: {output_path.parent}")
        sys.exit(1)
    
    if not output_path.parent.is_dir():
        print(f"Error: Output path parent is not a directory")
        sys.exit(1)
    
    print(f"Input file validated: {INPUT_FILE}")
    print(f"Output directory validated: {output_path.parent}")

def build_hierarchy(df, id_col, parent_col):

    children_map = defaultdict(list)
    
    id_to_row = {}
    
    for idx, row in df.iterrows():
        child_id = row[id_col]
        parent_id = row[parent_col]
        
        id_to_row[child_id] = row
        
        if pd.notna(parent_id):
            children_map[parent_id].append(child_id)
    
    return children_map, id_to_row

def get_all_descendants(root_id, children_map, id_to_row):

    if root_id not in id_to_row:
        return None, []
    
    result_rows = []
    queue = [root_id]
    visited = set()
    
    result_rows.append(id_to_row[root_id])
    visited.add(root_id)
    
    while queue:
        current_id = queue.pop(0)
        
        if current_id in children_map:
            children = children_map[current_id]
            
            for child_id in children:
                if child_id not in visited:
                    result_rows.append(id_to_row[child_id])
                    queue.append(child_id)
                    visited.add(child_id)
    
    return result_rows

def process_hierarchy():

    print("\n" + "="*70)
    print("CSV PARENT-CHILD HIERARCHY EXTRACTOR")
    print("="*70)
    
    validate_paths()
    
    print(f"\n Configuration:")
    print(f"Input:  {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"ID Column: {ID_COLUMN}")
    print(f"Parent ID Column: {PARENT_ID_COLUMN}")
    print(f"Root ID: {ROOT_ID}")
    print(f"Chunk size: {CHUNK_SIZE:,} rows")
    
    print(f"\nReading CSV file...")
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df):,} rows")
        
        if ID_COLUMN not in df.columns:
            print(f"Error: Column '{ID_COLUMN}' not found in CSV")
            print(f"Available columns: {list(df.columns)}")
            sys.exit(1)
        
        if PARENT_ID_COLUMN not in df.columns:
            print(f"Error: Column '{PARENT_ID_COLUMN}' not found in CSV")
            print(f"Available columns: {list(df.columns)}")
            sys.exit(1)
        
        print(f"\n Building hierarchy...")
        children_map, id_to_row = build_hierarchy(df, ID_COLUMN, PARENT_ID_COLUMN)
        print(f"Built hierarchy with {len(id_to_row):,} unique IDs")
        print(f"Found {len(children_map)} parent nodes")
        
        if ROOT_ID not in id_to_row:
            print(f"\nError: Root ID '{ROOT_ID}' not found in CSV")
            print(f"Available sample IDs: {list(id_to_row.keys())[:10]}")
            sys.exit(1)
        
        print(f"\nExtracting descendants of '{ROOT_ID}'...")
        result_rows = get_all_descendants(ROOT_ID, children_map, id_to_row)
        
        if not result_rows:
            print(f"Error: No descendants found for ID '{ROOT_ID}'")
            sys.exit(1)
        
        output_df = pd.DataFrame(result_rows)
        
        print(f"Writing output file...")
        output_df.to_csv(OUTPUT_FILE, index=False)
        
        print("\n" + "="*70)
        print("SUCCESS")
        print("="*70)
        print(f"Root ID:           {ROOT_ID}")
        print(f"Rows extracted:    {len(output_df):,}")
        print(f"Output columns:    {len(output_df.columns)}")
        print(f"\nFirst 5 rows:")
        print(output_df.head(5).to_string())
        print(f"\n...")
        print(f"\nLast 5 rows:")
        print(output_df.tail(5).to_string())
        print(f"\nOutput file: {OUTPUT_FILE}")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\nError processing file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    process_hierarchy()