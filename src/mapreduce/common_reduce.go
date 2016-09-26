package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		file := reduceName(jobName, m, reduceTaskNumber)

		fin, err := os.Open(file)
		defer fin.Close()
		if err != nil {
			fmt.Println(err)
			return
		}

		dec := json.NewDecoder(fin)

		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = make([]string, 0)
			}

			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}

	}

	var keys []string
	for k, _ := range kvs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	file_res := mergeName(jobName, reduceTaskNumber)
	//fmt.Println(file_res)
	fout, err := os.Create(file_res)
	defer fout.Close()
	if err != nil {
		fmt.Println("create file error")
		return
	}
	enc := json.NewEncoder(fout)

	for i := 0; i < len(keys); i++ {
		err = enc.Encode(&KeyValue{keys[i], reduceF(keys[i], kvs[keys[i]])})
		if err != nil {
			panic("json encode failed!")
		}
	}

}
