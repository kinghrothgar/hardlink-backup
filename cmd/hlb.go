package main

import (
	"fmt"
	"os"
	"path/filepath"
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
				inodes.Upsert(strconv.FormatUint(stat.Ino, 10), []string{path}, updateInodes)
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
	jobs := make(chan string, 100)
	inodes := cmap.New[[]string]()
	var wg sync.WaitGroup
	//Start as many workers you want, now 10 workers
	for w := 1; w <= 10; w++ {
		go loopFilesWorker(&wg, jobs, inodes)
	}
	//Start the recursion
	LoopDirsFiles(&wg, jobs, "/media/bigdata")
	wg.Wait()
	inodes.IterCb(func(k string, v []string) {
		fmt.Printf("%s: %d", k, len(v))
	})
}
