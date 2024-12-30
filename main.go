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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// Create error channel
	errChan := make(chan error, 1)

	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Handle signals in a separate goroutine
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		cancel()
		// Force exit after 5 seconds if graceful shutdown fails
		time.AfterFunc(5*time.Second, func() {
			log.Println("Force exiting due to timeout")
			os.Exit(1)
		})
	}()

	if *workerMode {
		runWorker(ctx, cancel, sigChan, errChan, *coordinatorAddr)
	} else {
		runCoordinator(ctx, cancel, sigChan, errChan, *nReduce, *inputFiles, *intermediateDir, *coordinatorAddr)
	}

	// Wait for error or shutdown
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

func runWorker(ctx context.Context, cancel context.CancelFunc, sigChan chan os.Signal, errChan chan error, coordinatorAddr string) {
	log.Printf("Starting worker process...")

	worker := distributed.NewWorker(
		&map_reduce.WordCountMapper{},
		&map_reduce.WordCountReducer{},
	)

	var wg sync.WaitGroup
	wg.Add(1)

	// Handle shutdown signal
	go func() {
		defer wg.Done()
		select {
		case sig := <-sigChan:
			log.Printf("Received signal %v, shutting down...", sig)
			cancel()
		case <-ctx.Done():
			log.Printf("Context done, worker shutting down...")
		}
	}()

	// Start worker with error handling
	go func() {
		err := worker.Register(coordinatorAddr, ctx)
		if err == nil ||
			err == distributed.ErrCleanShutdown ||
			strings.Contains(err.Error(), "RPC timeout") {
			log.Printf("Worker completed successfully")
			errChan <- nil
		} else {
			log.Printf("Worker error: %v", err)
			errChan <- err
		}
		cancel()
	}()

	<-ctx.Done()
	wg.Wait()
	log.Printf("Worker shutdown complete")
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
