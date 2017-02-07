package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
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
	reduceOutputName := mergeName(jobName, reduceTaskNumber)
	routputfile, err := os.Create(reduceOutputName)
	//var m map[string][]string = make(map[string][]string)
	checkError(err)
	defer routputfile.Close()
	keyValMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		fmt.Println("The reduced task fileName is ", fileName)
		dat, err := ioutil.ReadFile(fileName)
		checkError(err)
		dec := json.NewDecoder(strings.NewReader(string(dat)))
		for {
			var m KeyValue
			err = dec.Decode(&m)
			if err == io.EOF {
				break
			} else {
				checkError(err)
			}
			keyValMap[m.Key] = append(keyValMap[m.Key], m.Value)
		}
	}

	enc := json.NewEncoder(routputfile)
	for k, v := range keyValMap {
		err := enc.Encode(KeyValue{k, reduceF(k, v)})
		checkError(err)
	}

}
