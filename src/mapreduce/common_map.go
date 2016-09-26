package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	buff, err := ioutil.ReadFile(inFile)
	if err != nil {
		panic(err)
	}

	key_value := mapF(inFile, string(buff))

	for j := 0; j < nReduce; j++ {
		partition_file := reduceName(jobName, mapTaskNumber, j)
		fout, err := os.Create(partition_file)
		defer fout.Close()
		if err != nil {
			fmt.Println(partition_file, err)
			return
		}
		enc := json.NewEncoder(fout)
		for _, kv := range key_value {
			if ihash(kv.Key)%uint32(nReduce) == uint32(j) {

				err = enc.Encode(&kv)
				if err != nil {
					panic("json encode failed!")
				}
			}
		}
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
