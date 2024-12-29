package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ogzhanolguncu/map-reduce-in-go/distributed"
	"github.com/ogzhanolguncu/map-reduce-in-go/map_reduce"
)

func main() {
	var (
		workerMode      = flag.Bool("worker", false, "Run as worker")
		coordinatorAddr = flag.String("coordinator", "", "Coordinator address")
		nReduce         = flag.Int("reduce", 5, "Number of reduce tasks")
		inputFiles      = flag.String("input", "", "Comma-separated list of input files")
		intermediateDir = flag.String("intermediate-dir", "", "Directory for intermediate files")
	)
	flag.Parse()

	// Create context with timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(sigChan)

	// Create error channel
	errChan := make(chan error, 1)

	if *workerMode {
		runWorker(ctx, cancel, sigChan, errChan, *coordinatorAddr)
	} else {
		runCoordinator(ctx, cancel, sigChan, errChan, *nReduce, *inputFiles, *intermediateDir, *coordinatorAddr)
	}

	// Handle errors or shutdown
	select {
	case err := <-errChan:
		if err != nil {
			log.Printf("Error: %v", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		log.Println("Shutdown complete")
	}
}

func runWorker(ctx context.Context, cancel context.CancelFunc, sigChan chan os.Signal, errChan chan error, coordinatorAddr string) {
	if coordinatorAddr == "" {
		errChan <- fmt.Errorf("coordinator address required for worker mode")
		return
	}

	worker := distributed.NewWorker(
		&map_reduce.WordCountMapper{},
		&map_reduce.WordCountReducer{},
	)

	// Handle shutdown signal
	go func() {
		<-sigChan
		log.Println("Shutting down worker...")
		cancel()
	}()

	// Start worker with retry logic
	go func() {
		const maxRetries = 3
		for retry := 0; retry < maxRetries; retry++ {
			if retry > 0 {
				log.Printf("Retrying connection to coordinator (attempt %d/%d)...", retry+1, maxRetries)
				time.Sleep(time.Second * time.Duration(retry+1))
			}

			err := worker.Register(coordinatorAddr, ctx)
			if err == nil {
				errChan <- nil
				return
			}

			if ctx.Err() != nil {
				errChan <- ctx.Err()
				return
			}

			log.Printf("Worker failed to start: %v", err)
		}
		errChan <- fmt.Errorf("failed to connect to coordinator after %d attempts", maxRetries)
	}()
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
	wg.Add(1)

	// Handle shutdown
	go func() {
		defer wg.Done()
		select {
		case sig := <-sigChan:
			log.Printf("Received signal: %v", sig)
		case <-ctx.Done():
			log.Println("Context cancelled")
		}

		log.Println("Initiating graceful shutdown...")
		coordinator.Cleanup()
		cancel()
	}()

	// Monitor job completion separately
	go func() {
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
