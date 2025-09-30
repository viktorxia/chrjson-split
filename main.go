package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/tidwall/gjson"
)

const UnknownChr = "unknown_chr"

// ChromosomeProcessor is a processor for chromosome-specific JSONL files
type ChromosomeProcessor struct {
	inputFile     string
	prefix        string
	chrFieldName  string
	chrNames      []string
	chrSet        map[string]bool
	outputWriters map[string]*bufio.Writer
	outputFiles   map[string]*os.File
}

// NewChromosomeProcessor is the constructor for ChromosomeProcessor
func NewChromosomeProcessor(inputFile, prefix, chrFieldName string, chrNames []string) *ChromosomeProcessor {
	chrSet := make(map[string]bool)
	for _, chr := range chrNames {
		chrSet[chr] = true
	}

	return &ChromosomeProcessor{
		inputFile:     inputFile,
		prefix:        prefix,
		chrFieldName:  chrFieldName,
		chrNames:      chrNames,
		chrSet:        chrSet,
		outputWriters: make(map[string]*bufio.Writer),
		outputFiles:   make(map[string]*os.File),
	}
}

// InitializeOutputFiles creates output files for each chromosome
func (cp *ChromosomeProcessor) InitializeOutputFiles() error {

	allChrs := append(cp.chrNames, UnknownChr)

	for _, chr := range allChrs {
		filename := fmt.Sprintf("%s_%s.jsonl", cp.prefix, chr)

		file, err := os.Create(filename)
		if err != nil {
			cp.CloseAllFiles()
			return fmt.Errorf("failed to create output file %s: %v", filename, err)
		}

		writer := bufio.NewWriterSize(file, 4*1024*1024)

		cp.outputFiles[chr] = file
		cp.outputWriters[chr] = writer
	}

	return nil
}

// GetOutputWriter gets the output writer for the specified chromosome
func (cp *ChromosomeProcessor) GetOutputWriter(chr string) *bufio.Writer {
	if writer, exists := cp.outputWriters[chr]; exists {
		return writer
	}
	return cp.outputWriters[UnknownChr]
}

// ExtractChromosome extracts the chromosome information from one row
func (cp *ChromosomeProcessor) ExtractChromosome(line []byte) (string, bool) {
	result := gjson.GetBytes(line, cp.chrFieldName)
	if !result.Exists() {
		return "", false
	}
	return result.String(), true
}

// ProcessFile processes the input file
func (cp *ChromosomeProcessor) ProcessFile() error {
	fmt.Printf("Processing: %s -> %s_*.jsonl\n", cp.inputFile, cp.prefix)

	if err := cp.InitializeOutputFiles(); err != nil {
		return err
	}
	defer cp.CloseAllFiles()

	file, err := os.Open(cp.inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// !!! row of data may be too large, set buffer size to 10MB
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		chr, found := cp.ExtractChromosome(line)

		outputChr := UnknownChr
		if found && cp.chrSet[chr] {
			outputChr = chr
		}

		writer := cp.GetOutputWriter(outputChr)
		if _, err := writer.Write(line); err != nil {
			return fmt.Errorf("failed to write to output file at line %d: %v", lineNum, err)
		}
		if err := writer.WriteByte('\n'); err != nil {
			return fmt.Errorf("failed to write newline at line %d: %v", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file at line %d: %v", lineNum, err)
	}
	cp.FlushAllWriters()

	return nil
}

// FlushAllWriters flushes all output writers
func (cp *ChromosomeProcessor) FlushAllWriters() {
	for _, writer := range cp.outputWriters {
		writer.Flush()
	}
}

// CloseAllFiles closes all output files
func (cp *ChromosomeProcessor) CloseAllFiles() {
	cp.FlushAllWriters()
	for _, file := range cp.outputFiles {
		file.Close()
	}
}

// getDefaultChromosomes returns the default list of chromosome names
func getDefaultChromosomes() []string {
	chroms := make([]string, 0, 25)

	// chr1-chr22
	for i := 1; i <= 22; i++ {
		chroms = append(chroms, fmt.Sprintf("chr%d", i))
	}

	// chrX, chrY, chrM
	chroms = append(chroms, "chrX", "chrY", "chrM")

	return chroms
}

// parseChromosomeNames parses the comma-separated chromosome names string
func parseChromosomeNames(chrNamesStr string) []string {
	if chrNamesStr == "" {
		return getDefaultChromosomes()
	}

	parts := strings.Split(chrNamesStr, ",")
	chrNames := make([]string, 0, len(parts))

	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name != "" {
			chrNames = append(chrNames, name)
		}
	}

	return chrNames
}

func main() {

	startTime := time.Now()

	// parse command line options
	var (
		inputFile    = pflag.StringP("input", "i", "", "Input JSONL file path (required)")
		prefix       = pflag.String("prefix", "output", "Output file prefix")
		chrFieldName = pflag.String("chr-field-name", "chr", "Chromosome field name in JSON")
		chrNamesStr  = pflag.StringP("chr-names", "c", "", "Custom chromosome names (comma-separated)")
		help         = pflag.BoolP("help", "h", false, "Show help message")
	)

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "A tool to split a JSONL/NDJSON file by chromosome\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		pflag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s --input input.jsonl --prefix output\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i input.jsonl --prefix output\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i data.jsonl  --prefix result --chr-field-name chromosome\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i data.jsonl  --chr-names \"chr1,chr2,chrX\"\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -i data.jsonl  -c \"chr1,chr2,chrX\" --prefix my_output\n", os.Args[0])
	}

	pflag.Parse()

	if *help {
		pflag.Usage()
		os.Exit(0)
	}

	// validate options
	if *inputFile == "" {
		fmt.Fprintf(os.Stderr, "Error: Input file is required\n\n")
		pflag.Usage()
		os.Exit(1)
	}

	if _, err := os.Stat(*inputFile); os.IsNotExist(err) {
		log.Fatalf("Error: Input file does not exist: %s", *inputFile)
	}

	// parse chromosome names
	chrNames := parseChromosomeNames(*chrNamesStr)

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Input file: %s\n", *inputFile)
	fmt.Printf("  Output prefix: %s\n", *prefix)
	fmt.Printf("  Chromosome field: %s\n", *chrFieldName)
	fmt.Printf("  Target chromosomes: %v\n", chrNames)
	fmt.Println()

	processor := NewChromosomeProcessor(*inputFile, *prefix, *chrFieldName, chrNames)
	if err := processor.ProcessFile(); err != nil {
		log.Fatalf("Error processing file: %v", err)
	} else {
		fmt.Printf("Finished in %.2f s\n", time.Since(startTime).Seconds())
	}

}
