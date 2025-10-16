# dataset_merger.py
files = [
    "dataset_soccer_top_all_250.jsonl",
    "dataset_soccer_top_year_250.jsonl",
    "dataset_soccercirclejerk_top_all_250.jsonl",
    "dataset_soccercirclejerk_top_month_250.jsonl",
    "dataset_soccercirclejerk_top_year_250.jsonl",
]  # list of files to merge

output_file = "merged_datasets.jsonl"

with open(output_file, "w", encoding="utf-8") as outfile:
    for fname in files:
        with open(fname, "r", encoding="utf-8") as infile:
            outfile.write(infile.read())
            outfile.write("\n")  # optional: add newline between files

print(f"Merged {len(files)} files into {output_file}")
