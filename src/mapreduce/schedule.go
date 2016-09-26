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
    for  i:= 0; i < ntasks; i += len(mr.workers) {
		mr.Mutex.Lock()
        for j, worker := range mr.workers{
            doTaskArgs := DoTaskArgs{mr.jobName, mr.files[i + j], phase, i + j , nios}
            ok := call(worker, "Worker.DoTask", doTaskArgs, new(struct{}))
            if !ok {
				i--
                continue
                fmt.Println("call worker error!")
            }
        }
		mr.Mutex.Unlock()
    }


	fmt.Printf("Schedule: %v phase done\n", phase)
}
