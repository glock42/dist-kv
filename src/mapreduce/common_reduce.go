package mapreduce
import (
	"fmt"
	"os"
	"encoding/json"
	"sort"
)

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	var kvs []KeyValue
	for m:=0; m < nMap; m++{
		file := reduceName(jobName, m, reduceTaskNumber)

		fin,err := os.Open(file)
		defer fin.Close()
		if err != nil {
		    fmt.Println(err)
		    return
		}

		dec := json.NewDecoder(fin)
		
		for{
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		//fmt.Println(kvs)
	}

	sort.Sort(ByKey(kvs))
		
	file_res := mergeName(jobName, reduceTaskNumber)
	//fmt.Println(file_res)
	fout, err := os.Create(file_res)
	defer fout.Close()
	if err != nil {
	    fmt.Println("create file error")
	    return
	}
	enc := json.NewEncoder(fout)
	
	var vals []string
	vals = append(vals, kvs[0].Value)
	for i:=0;i<len(kvs)-1;i++ {
		if kvs[i].Key == kvs[i+1].Key {
			vals = append(vals, kvs[i+1].Value)
		} else{
			err = enc.Encode(KeyValue{kvs[i].Key,reduceF(kvs[i].Key,vals)})
	    	if err != nil {
				panic("json encode failed!")
			}

			vals = make([]string, 0)
			vals = append(vals, kvs[i+1].Value)
		}
	}
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
