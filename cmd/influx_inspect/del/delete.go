// Package del performs bulk offline shard data deletion.
package del

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/tsdb"
)

// Command represents the program execution for "influx_inspect delete".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	path string
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) (err error) {
	fs := flag.NewFlagSet("influx-inspect-delete", flag.ExitOnError)
	measurement := fs.String("m", "", "Measurement name")
	start := fs.String("start", "", "Start time")
	end := fs.String("end", "", "End time")
	seriesDir := fs.String("seriesdir", "", "series file directory")
	dataDir := fs.String("datadir", "", "shard data directory")
	walDir := fs.String("waldir", "", "shard WAL directory")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		fmt.Printf("shard path required\n\n")
		fs.Usage()
		return nil
	}

	// Parse start/end timestamps.
	if cmd.start, err = time.Parse(time.RFC3339, *start); err != nil {
		fmt.Printf("invalid start time\n\n")
		fs.Usage()
		return nil
	} else if cmd.end, err = time.Parse(time.RFC3339, *end); err != nil {
		fmt.Printf("invalid end time\n\n")
		fs.Usage()
		return nil
	}

	// Open series file.
	if path.Base(*seriesDir) != tsdb.SeriesFileDirectory {
		return errors.New("invalid series file path")
	}
	sfile := tsdb.NewSeriesFile(*seriesDir)
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	// Open shard.
	id, err := strconv.Atoi(*dataDir)
	if err != nil {
		return errors.New("invalid shard data path")
	} else if _, err := strconv.Atoi(*walDir); err != nil {
		return errors.New("invalid shard WAL path")
	}
	sh := tsdb.NewShard(id, *dataDir, *walDir, sfile, opt)

	return cmd.dump()
}

func (cmd *Command) dump() error {
	var errors []error

	f, err := os.Open(cmd.path)
	if err != nil {
		return err
	}

	// Get the file size
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	b := make([]byte, 8)

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		return fmt.Errorf("Error opening TSM files: %s", err.Error())
	}
	defer r.Close()

	minTime, maxTime := r.TimeRange()
	keyCount := r.KeyCount()

	blockStats := &blockStats{}

	println("Summary:")
	fmt.Printf("  File: %s\n", cmd.path)
	fmt.Printf("  Time Range: %s - %s\n",
		time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
	)
	fmt.Printf("  Duration: %s ", time.Unix(0, maxTime).Sub(time.Unix(0, minTime)))
	fmt.Printf("  Series: %d ", keyCount)
	fmt.Printf("  File Size: %d\n", stat.Size())
	println()

	tw := tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)

	if cmd.dumpIndex {
		println("Index:")
		tw.Flush()
		println()

		fmt.Fprintln(tw, "  "+strings.Join([]string{"Pos", "Min Time", "Max Time", "Ofs", "Size", "Key", "Field"}, "\t"))
		var pos int
		for i := 0; i < keyCount; i++ {
			key, _ := r.KeyAt(i)
			for _, e := range r.Entries(key) {
				pos++
				split := strings.Split(string(key), "#!~#")

				// Possible corruption? Try to read as much as we can and point to the problem.
				measurement := split[0]
				field := split[1]

				if cmd.filterKey != "" && !strings.Contains(string(key), cmd.filterKey) {
					continue
				}
				fmt.Fprintln(tw, "  "+strings.Join([]string{
					strconv.FormatInt(int64(pos), 10),
					time.Unix(0, e.MinTime).UTC().Format(time.RFC3339Nano),
					time.Unix(0, e.MaxTime).UTC().Format(time.RFC3339Nano),
					strconv.FormatInt(int64(e.Offset), 10),
					strconv.FormatInt(int64(e.Size), 10),
					measurement,
					field,
				}, "\t"))
				tw.Flush()
			}
		}
	}

	tw = tabwriter.NewWriter(cmd.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "  "+strings.Join([]string{"Blk", "Chk", "Ofs", "Len", "Type", "Min Time", "Points", "Enc [T/V]", "Len [T/V]"}, "\t"))

	// Starting at 5 because the magic number is 4 bytes + 1 byte version
	i := int64(5)
	var blockCount, pointCount, blockSize int64
	indexSize := r.IndexSize()

	// Start at the beginning and read every block
	for j := 0; j < keyCount; j++ {
		key, _ := r.KeyAt(j)
		for _, e := range r.Entries(key) {

			f.Seek(int64(e.Offset), 0)
			f.Read(b[:4])

			chksum := binary.BigEndian.Uint32(b[:4])

			buf := make([]byte, e.Size-4)
			f.Read(buf)

			blockSize += int64(e.Size)

			if cmd.filterKey != "" && !strings.Contains(string(key), cmd.filterKey) {
				i += blockSize
				blockCount++
				continue
			}

			blockType := buf[0]

			encoded := buf[1:]

			var v []tsm1.Value
			v, err := tsm1.DecodeBlock(buf, v)
			if err != nil {
				return err
			}
			startTime := time.Unix(0, v[0].UnixNano())

			pointCount += int64(len(v))

			// Length of the timestamp block
			tsLen, j := binary.Uvarint(encoded)

			// Unpack the timestamp bytes
			ts := encoded[int(j) : int(j)+int(tsLen)]

			// Unpack the value bytes
			values := encoded[int(j)+int(tsLen):]

			tsEncoding := timeEnc[int(ts[0]>>4)]
			vEncoding := encDescs[int(blockType+1)][values[0]>>4]

			typeDesc := blockTypes[blockType]

			blockStats.inc(0, ts[0]>>4)
			blockStats.inc(int(blockType+1), values[0]>>4)
			blockStats.size(len(buf))

			if cmd.dumpBlocks {
				fmt.Fprintln(tw, "  "+strings.Join([]string{
					strconv.FormatInt(blockCount, 10),
					strconv.FormatUint(uint64(chksum), 10),
					strconv.FormatInt(i, 10),
					strconv.FormatInt(int64(len(buf)), 10),
					typeDesc,
					startTime.UTC().Format(time.RFC3339Nano),
					strconv.FormatInt(int64(len(v)), 10),
					fmt.Sprintf("%s/%s", tsEncoding, vEncoding),
					fmt.Sprintf("%d/%d", len(ts), len(values)),
				}, "\t"))
			}

			i += blockSize
			blockCount++
		}
	}

	if cmd.dumpBlocks {
		println("Blocks:")
		tw.Flush()
		println()
	}

	var blockSizeAvg int64
	if blockCount > 0 {
		blockSizeAvg = blockSize / blockCount
	}
	fmt.Printf("Statistics\n")
	fmt.Printf("  Blocks:\n")
	fmt.Printf("    Total: %d Size: %d Min: %d Max: %d Avg: %d\n",
		blockCount, blockSize, blockStats.min, blockStats.max, blockSizeAvg)
	fmt.Printf("  Index:\n")
	fmt.Printf("    Total: %d Size: %d\n", blockCount, indexSize)
	fmt.Printf("  Points:\n")
	fmt.Printf("    Total: %d", pointCount)
	println()

	println("  Encoding:")
	for i, counts := range blockStats.counts {
		if len(counts) == 0 {
			continue
		}
		fmt.Printf("    %s: ", strings.Title(fieldType[i]))
		for j, v := range counts {
			fmt.Printf("\t%s: %d (%d%%) ", encDescs[i][j], v, int(float64(v)/float64(blockCount)*100))
		}
		println()
	}
	fmt.Printf("  Compression:\n")
	fmt.Printf("    Per block: %0.2f bytes/point\n", float64(blockSize)/float64(pointCount))
	fmt.Printf("    Total: %0.2f bytes/point\n", float64(stat.Size())/float64(pointCount))

	if len(errors) > 0 {
		println()
		fmt.Printf("Errors (%d):\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  * %v\n", err)
		}
		println()
		return fmt.Errorf("error count %d", len(errors))
	}
	return nil
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	usage := `Performs offline bulk deletion in a single shard.

Usage: influx_inspect delete [flags] path

    -measurement 
            Measurement name to delete.
    -start TIME
            Start timestamp in ISO-8601 format.
    -end TIME
            End timestamp in ISO-8601 format.
`

	fmt.Fprintf(cmd.Stdout, usage)
}

func (cmd *Command) isShardDir(dir string) error {
	name := filepath.Base(dir)
	if id, err := strconv.Atoi(name); err != nil || id < 1 {
		return fmt.Errorf("not a valid shard dir: %v", dir)
	}

	return nil
}
