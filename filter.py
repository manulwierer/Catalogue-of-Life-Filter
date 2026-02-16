import pandas as pd
import sys
from pathlib import Path
                                    #or .csv
INPUT_TSV = Path(r"yourinputpath\example.tsv")
OUTPUT_CSV = Path(r"youroutputpath\example.csv")

CHUNK_SIZE = 50_000

#suggestion
#COLUMNS_TO_REMOVE = []
COLUMNS_TO_REMOVE = ["clb:merged","col:accordingToID","col:accordingToPage","col:accordingToPageLink","col:alternativeID","col:authorship","col:basionymAuthorship","col:basionymAuthorshipID","col:basionymAuthorshipYear","col:basionymExAuthorship","col:basionymExAuthorshipID","col:basionymID","col:branchLength","col:class","col:code","col:combinationAuthorship","col:combinationAuthorshipID","col:combinationAuthorshipYear","col:combinationExAuthorship","col:combinationExAuthorshipID","col:cultivarEpithet","col:environment","col:etymology","col:extinct","col:family","col:gender","col:genderAgreement","col:genericName","col:genus","col:infragenericEpithet","col:infraspecificEpithet","col:kingdom","col:link","col:modified","col:modifiedBy","col:nameAlternativeID","col:namePhrase","col:nameReferenceID","col:nameRemarks","col:nameStatus","col:notho","col:order","col:ordinal","col:originalSpelling","col:phylum","col:publishedInPage","col:publishedInPageLink","col:publishedInYear","col:referenceID","col:remarks","col:scrutinizer","col:scrutinizerDate","col:scrutinizerID","col:section","col:sourceID","col:specificEpithet","col:species","col:status","col:subclass","col:subfamily","col:subgenus","col:suborder","col:subphylum","col:subtribe","col:superfamily","col:temporalRangeEnd","col:temporalRangeStart","col:tribe","col:uninomial"]

ROW_FILTERS = {
#suggestion
                                "col:status": ["synonym", "ambiguous synonym", "provisionally accepted"],

#rank filters:                                
#up2butnotincluding baseline:    
"col:rank":   ["other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding species:     
#"col:rank":   ["subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding genus:       
#"col:rank":   ["subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding tribe:       
#"col:rank":   ["subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding family:      
#"col:rank":   ["subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding order:       
#"col:rank":   ["suborder","infraorder","parvorder","nanorder","superfamily","epifamily","family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding class:       
#"col:rank":   ["subclass","infraclass","subterclass","superorder","order","suborder","infraorder","parvorder","nanorder","superfamily","epifamily","family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butnotincluding phylum:      
#"col:rank":   ["subphylum","infraphylum","parvphylum","gigaclass","megaclass","superclass","class","subclass","infraclass","subterclass","superorder","order","suborder","infraorder","parvorder","nanorder","superfamily","epifamily","family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]

#up2butandincluding baseline:    
#"col:rank":   ["other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding species:     
#"col:rank":   ["species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding genus:       
#"col:rank":   ["genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding tribe:       
#"col:rank":   ["tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding family:      
#"col:rank":   ["family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding order:       
#"col:rank":   ["order","suborder","infraorder","parvorder","nanorder","superfamily","epifamily","family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding class:       
#"col:rank":   ["class","subclass","infraclass","subterclass","superorder","order","suborder","infraorder","parvorder","nanorder","superfamily","epifamily","family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
#up2butandincluding phylum:      
#"col:rank":   ["phylum","subphylum","infraphylum","parvphylum","gigaclass","megaclass","superclass","class","subclass","infraclass","subterclass","superorder","order","suborder","infraorder","parvorder","nanorder","superfamily","epifamily","family","subfamily","infrafamily","supertribe","tribe","subtribe","infratribe","genus","subgenus","series","species","subspecies","variety","subvariety","form","other","proles","mutatio","lusus","abberation","forma specialis","subform","infrasubspecific name","infraspecific name","species aggregate","subsection zoology","section zoology","subsection botany","section botany","infrageneric name","unranked","natio",""]
}

def validate_paths(input_path: Path, output_path: Path) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not output_path.parent.is_dir():
        raise NotADirectoryError(f"Output directory is not a directory: {output_path.parent}")

def get_tsv_columns(tsv_path: Path) -> list[str]:
    return list(pd.read_csv(tsv_path, sep="\t", nrows=0).columns)

def apply_row_filters(df: pd.DataFrame, filters: dict[str, list[str]]) -> pd.DataFrame:
    if not filters:
        return df

    for col, bad_values in filters.items():
        if col not in df.columns:
            print(f"Warning: Filter column '{col}' not found; skipping this filter.")
            continue
        df = df[~df[col].isin(bad_values)]
    return df

def remove_columns(df: pd.DataFrame, columns_to_remove: list[str]) -> pd.DataFrame:
    if not columns_to_remove:
        return df

    present = [c for c in columns_to_remove if c in df.columns]
    missing = [c for c in columns_to_remove if c not in df.columns]
    if missing:
        print(f"Warning: Columns not found (cannot remove): {missing}")

    if present:
        df = df.drop(columns=present)
    return df

def process_tsv_to_filtered_csv() -> None:
    print("\n" + "=" * 70)
    print("TSV → row-filter → remove columns → CSV (chunked, single pass)")
    print("=" * 70)

    validate_paths(INPUT_TSV, OUTPUT_CSV)

    all_cols = get_tsv_columns(INPUT_TSV)

    filter_cols = [c for c in ROW_FILTERS.keys() if c in all_cols]
    keep_for_output = [c for c in all_cols if c not in set(COLUMNS_TO_REMOVE)]
    usecols = keep_for_output + [c for c in filter_cols if c not in keep_for_output]

    if not usecols:
        raise ValueError("No columns selected to read (check COLUMNS_TO_REMOVE vs TSV header).")

    print(f"Input:  {INPUT_TSV}")
    print(f"Output: {OUTPUT_CSV}")
    print(f"Chunk size: {CHUNK_SIZE:,}")
    print(f"Columns in TSV: {len(all_cols)}")
    print(f"Columns read (after optimization): {len(usecols)}")

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    total_in = 0
    total_out = 0
    first_write = True

    try:
        reader = pd.read_csv(
            INPUT_TSV,
            sep="\t",
            usecols=usecols,
            dtype=str,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )

        for i, chunk in enumerate(reader, start=1):
            total_in += len(chunk)

            chunk = apply_row_filters(chunk, ROW_FILTERS)
            chunk = remove_columns(chunk, COLUMNS_TO_REMOVE)

            chunk.to_csv(
                OUTPUT_CSV,
                index=False,
                mode="w" if first_write else "a",
                header=first_write,
            )
            first_write = False

            total_out += len(chunk)
            if i % 10 == 0:
                print(f"Processed {i} chunks | input: {total_in:,} | output: {total_out:,}")

        print("\n" + "=" * 70)
        print("success")
        print("=" * 70)
        print(f"Input rows:   {total_in:,}")
        print(f"Output rows:  {total_out:,}")
        print(f"Rows removed: {total_in - total_out:,}")
        print(f"Output file:  {OUTPUT_CSV}")
        print("=" * 70 + "\n")

    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    process_tsv_to_filtered_csv()
