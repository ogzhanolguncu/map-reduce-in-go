package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/map-reduce-in-go/distributed"
	"github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
)

func main() {
	var (
		coordinatorMode = flag.Bool("coordinator", false, "Run as coordinator")
		coordinatorAddr = flag.String("addr", "", "Coordinator address")
		nReduce         = flag.Int("reduce", 5, "Number of reduce tasks")
		inputFiles      = flag.String("input", "", "Comma-separated list of input files")
		intermediateDir = flag.String("intermediate-dir", "", "Directory for intermediate files")
		nWorkers        = flag.Int("workers", runtime.NumCPU(), "Number of workers to spawn (defaults to number of CPU cores)")
	)
	flag.Parse()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	errChan := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())

	// Handle signals
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		cancel()
		time.AfterFunc(5*time.Second, func() {
			log.Println("Force exiting due to timeout")
			os.Exit(1)
		})
	}()

	if *coordinatorMode {
		runCoordinator(ctx, cancel, sigChan, errChan, *nReduce, *inputFiles, *intermediateDir, *coordinatorAddr)
	} else {
		// Spawn multiple workers
		runWorkers(ctx, cancel, sigChan, errChan, *coordinatorAddr, *nWorkers)
	}

	// Wait for completion or error
	select {
	case err := <-errChan:
		if err != nil {
			log.Printf("Error: %v", err)
			os.Exit(1)
		}
		log.Println("Clean shutdown")
	case <-ctx.Done():
		log.Println("Context cancelled, shutdown complete")
	case <-time.After(5 * time.Second):
		log.Println("Shutdown timed out, forcing exit")
		os.Exit(1)
	}
}

func runWorkers(ctx context.Context, cancel context.CancelFunc, sigChan chan os.Signal,
	errChan chan error, coordinatorAddr string, nWorkers int,
) {
	log.Printf("Starting %d worker processes...", nWorkers)

	var wg sync.WaitGroup
	workerErrors := make(chan error, nWorkers)

	// Start multiple workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()

			worker := distributed.NewWorker(
				&map_reduce.WordCountMapper{},
				&map_reduce.WordCountReducer{},
			)

			// Add small delay between worker starts to prevent registration conflicts
			time.Sleep(time.Duration(workerNum*100) * time.Millisecond)

			err := worker.Register(coordinatorAddr, ctx)
			if err != nil && err != distributed.ErrCleanShutdown &&
				!strings.Contains(err.Error(), "RPC timeout") {
				workerErrors <- fmt.Errorf("worker %d error: %v", workerNum, err)
				cancel() // Cancel other workers if one fails
			}
		}(i)
	}

	// Handle worker errors
	go func() {
		for err := range workerErrors {
			log.Printf("Worker error: %v", err)
			errChan <- err
		}
	}()

	// Wait for all workers to complete
	wg.Wait()
	close(workerErrors)

	log.Printf("All workers shutdown complete")
}

func runCoordinator(ctx context.Context, cancel context.CancelFunc, sigChan chan os.Signal,
	errChan chan error, nReduce int, inputFiles string, intermediateDir string, coordinatorAddr string,
) {
	if inputFiles == "" {
		errChan <- fmt.Errorf("input files required for coordinator mode")
		return
	}

	if coordinatorAddr == "" {
		errChan <- fmt.Errorf("coordinator address required")
		return
	}

	files := strings.Split(inputFiles, ",")
	coordinator := distributed.NewCoordinator(nReduce, files, intermediateDir)

	// Start coordinator
	if err := coordinator.Start(coordinatorAddr); err != nil {
		errChan <- fmt.Errorf("failed to start coordinator: %v", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2) // One for signal handling, one for job completion monitoring

	// Handle shutdown signals
	go func() {
		defer wg.Done()
		select {
		case sig := <-sigChan:
			log.Printf("Received signal: %v", sig)
			coordinator.Cleanup()
			cancel()
		case <-ctx.Done():
			log.Println("Context cancelled")
			coordinator.Cleanup()
		}
	}()

	// Monitor job completion
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if coordinator.IsComplete() {
					log.Println("All tasks completed")
					coordinator.Cleanup()
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown to complete
	wg.Wait()
	errChan <- nil
}
