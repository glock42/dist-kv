package mapreduce

import "fmt"

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

	isDoneCorrect := make(chan bool)
	//	workerNum := len(mr.workers)
	i := 0
	for i < ntasks {
		for worker := range mr.registerChannel {
			go func(i int, mr *Master, phase jobPhase, nios int) {
				doTaskArgs := DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
				ok := call(worker, "Worker.DoTask", doTaskArgs, new(struct{}))
				if !ok {
					fmt.Println("call worker error!")
					isDoneCorrect <- false
					return
				} else {
					isDoneCorrect <- true
					mr.registerChannel <- worker
				}
			}(i, mr, phase, nios)
			i++
			fmt.Println(worker)
			if !<-isDoneCorrect {
				i--
			}
			if i >= ntasks {
				break
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
