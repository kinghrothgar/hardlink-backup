package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"

	// TODO: docs make it seem like it should automatically be imported as cmap
	cmap "github.com/orcaman/concurrent-map/v2"
	log "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
}

// Because of the nature of the program, the newValue should only ever have 1, so appending to existing value is better
// Possibly test performance in the future
func updateInodes(exist bool, valueInMap []string, newValue []string) []string {
	a := make([]string, 0, len(newValue))
	for _, k := range newValue {
		a = append(a, k)
	}
	// TODO: is there a reason to check for nil?
	if exist && valueInMap != nil {
		return append(a, valueInMap...)
	}
	return a
}

func loopFilesWorker(wg *sync.WaitGroup, jobs chan string, inodes cmap.ConcurrentMap[string, []string]) error {
	for path := range jobs {
		files, err := os.ReadDir(path)
		if err != nil {
			log.WithError(err).Error("failed to ReadDir")
			wg.Done()
			return err
		}

		for _, file := range files {
			// TODO: handle links and shit
			if !file.IsDir() {
				info, err := file.Info()
				if err != nil {
					log.WithError(err).Warn("failed to get file info")
					continue
				}
				stat, ok := info.Sys().(*syscall.Stat_t)
				if !ok {
					log.Warn("failed to get syscall.Stat_t")
					continue
				}
				inodes.Upsert(strconv.FormatUint(stat.Ino, 10), []string{filepath.Join(path, file.Name())}, updateInodes)
			}
		}
		wg.Done()
	}
	return nil
}

func LoopDirsFiles(wg *sync.WaitGroup, jobs chan string, path string) error {
	files, err := os.ReadDir(path)
	if err != nil {
		log.WithError(err).Error("failed to ReadDir")
		return err
	}
	wg.Add(1)
	jobs <- path
	for _, file := range files {
		if file.IsDir() {
			//Recursively go further in the tree
			LoopDirsFiles(wg, jobs, filepath.Join(path, file.Name()))
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	path := flag.Arg(0)
	// TODO: do path validation such as existing and not containing more than one filesystem

	jobs := make(chan string, 100)
	inodes := cmap.New[[]string]()
	var wg sync.WaitGroup
	//Start as many workers you want, now 10 workers
	for w := 1; w <= 10; w++ {
		go loopFilesWorker(&wg, jobs, inodes)
	}
	//Start the recursion
	LoopDirsFiles(&wg, jobs, path)
	wg.Wait()
	// TODO: more optimized size?
	out := make([][]string, 0)
	inodes.IterCb(func(k string, v []string) {
		if len(v) == 1 {
			return
		}
		sort.Slice(v, func(i, j int) bool {
			return v[i] < v[j]
		})
		out = append(out, v)
	})
	// TODO: issue if out is len 0?
	sort.Slice(out, func(i, j int) bool {
		return out[i][0] < out[j][0]
	})
	jout, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.WithError(err).Fatal("failed to convert output to json")
	}
	fmt.Println(string(jout))
}
