package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
}

func loopFilesWorker(wg *sync.WaitGroup, jobs chan string) error {
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
				fmt.Println(file.Name())
				fmt.Printf("stat.Ino = %#v\n", stat.Ino)
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
	var wg sync.WaitGroup
	//Start as many workers you want, now 10 workers
	for w := 1; w <= 10; w++ {
		go loopFilesWorker(&wg, jobs)
	}
	//Start the recursion
	LoopDirsFiles(&wg, jobs, "/media/bigdata")
	wg.Wait()
}
