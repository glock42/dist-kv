package mapreduce

import "fmt"
import "sync"

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
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(i int, mr *Master, phase jobPhase, nios int) {
			defer wg.Done()
			for worker := range mr.registerChannel {

				doTaskArgs := DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
				ok := call(worker, "Worker.DoTask", doTaskArgs, new(struct{}))
				if !ok {
					fmt.Println("call worker error!")
					continue
				} else {
					go func() {
						mr.registerChannel <- worker
					}()
					return
				}
			}
		}(i, mr, phase, nios)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
