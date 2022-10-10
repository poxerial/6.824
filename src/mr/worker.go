package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key    string
	Values []string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	wid := -1
	result := ""
	args := Args{wid, Request, result}
	for {
		reply := Reply{}
		if call("Coordinator.Handle", &args, &reply) {
			if reply.TaskType == IsMap {
				//open and read task file
				filename := reply.Task
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
					file.Close()
					args = Args{reply.Wid, Fail, ""}
					continue
				}
				file.Close()

				// call map function
				kvs := mapf(filename, string(content))

				// shuffle
				m := make(map[string]*KeyValues)
				for _, kv := range kvs {
					_, ok := m[kv.Key]
					if !ok {
						kvs := KeyValues{kv.Key, make([]string, 0)}
						m[kv.Key] = &kvs
					}
					m[kv.Key].Values = append(m[kv.Key].Values, kv.Value)
				}

				// create immediate files
				enc := make([]json.Encoder, 0)
				files := make([]*os.File, 0)
				filesname := make([]string, 0)
				for i := 0; i < reply.NReduce; i++ {
					tempname := "/tmp/" + strconv.Itoa(ihash(filename)) + "-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(i)
					tmp, err := os.Create(tempname)
					if err != nil {
						fmt.Println("Can't create file", tempname)
						continue
					}
					files = append(files, tmp)
					filesname = append(filesname, tempname)
					enc = append(enc, *json.NewEncoder(tmp))
				}

				// write to immediate files in json format
				for k, v := range m {
					err := enc[ihash(k)%reply.NReduce].Encode(v)
					if err != nil {
						fmt.Println("Can't write to enc", enc[ihash(k)%reply.NReduce])
					}
				}

				// close immediate files
				for _, file := range files {
					err := file.Close()
					if err != nil {
						fmt.Println("Can't close file", file)
					}
				}

				//prepare args for next request
				args = Args{reply.Wid, MapFinish, strings.Join(filesname, ":")}
			} else if reply.TaskType == IsReduce {
				//open all immediate files and create decoders
				imms := strings.Split(reply.Task, ":")
				files := make([]*os.File, 0)
				dec := make([]*json.Decoder, 0)
				for _, filename := range imms {
					file, err := os.Open(filename)
					if err != nil {
						fmt.Println("Can't open file", filename)
						continue
					}
					files = append(files, file)
					dec = append(dec, json.NewDecoder(file))
				}

				// create map for shuffling and reducing
				m := make(map[string][]string)

				// read json format immediate files and shuffle
				for i := range dec {
					for {
						var kvs KeyValues
						if err := dec[i].Decode(&kvs); err != nil {
							break
						}
						_, ok := m[kvs.Key]
						if !ok {
							m[kvs.Key] = make([]string, 0)
						}
						m[kvs.Key] = append(m[kvs.Key], kvs.Values...)
					}
				}

				// close immediate files
				for _, file := range files {
					file.Close()
				}

				// call reduce function
				result := ""
				for key, val := range m {
					result += key + " " + reducef(key, val) + "\n"
				}

				// create output file and write
				output_name := "mr-out-" + strconv.Itoa(reply.Wid)
				output_file, err := os.Create(output_name)
				if err != nil {
					fmt.Print("Can't create file", output_name)
					args = Args{reply.Wid, Fail, ""}
					continue
				}
				output_file.Write([]byte(result))

				// prepare args for next request
				args = Args{reply.Wid, ReduceFinish, ""}
			} else if reply.TaskType == Exit {
				os.Exit(0)
			} else if reply.TaskType == Fail {
				args = Args{}
			} else if reply.TaskType == Wait {
				args = Args{}
				time.Sleep(time.Second)
			} else {
				log.Fatal("Invalid reply.TaskType:", reply.TaskType)
			}
		} else {
			log.Fatal("Can't remote process call", args)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
