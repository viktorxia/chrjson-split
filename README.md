# chrjson-split

A simple tool to split a very large JSONL file by chromosome.

Compile
```bash
/bin/bash build.sh
```

A command-line tool to split a JSONL file by chromosome.
```bash
./chrsplit -i "input.jsonl" --prefix "./split"
```

Specify chromosome field name and chromosome names (comma-separated without spaces)
```bash
./chrsplit -i "input.jsonl" --prefix "./split" \
 --chr-field-name "chr" \
 --chr-names "chr1,chr2,chr3,chr4,chr5,chr6,chr7,chr8,chr9,chr10,chr11,chr12,chr13,chr14,chr15,chr16,chr17,chr18,chr19,chr20,chr21,chr22,chrX,chrY,chrM"
```
