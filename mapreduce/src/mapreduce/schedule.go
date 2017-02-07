package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		go func() {
			fmt.Println("Pushing workers back")
			for _, worker := range mr.workers {
				fmt.Println("Worker is", worker)
				mr.registerChannel <- worker
			}
		}()
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		c := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fmt.Println("waiting for channel")
				v := <-mr.registerChannel
				if call(v, "Worker.DoTask", DoTaskArgs{mr.jobName, mr.files[c], phase, c, nios}, new(struct{})) {
					select {
					case mr.registerChannel <- v:
						fmt.Println("added to channel")
					default:
						fmt.Println("exiting")
					}
					break
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
