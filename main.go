// main.go
package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

const cpuProfile = false

// our approach is to build a trie of all the cities, and then walk it to get the results
type Node struct {
	Key      byte
	Children []*Node
	Min      int
	Max      int
	Sum      int
	Count    int
}

func (n *Node) Insert(value []byte) []byte {
	// recurse down the supplied value, creating nodes as needed
	cur := value[0]
	if cur == ';' {
		temp, rest := extractValue(value[1:])
		if temp < n.Min || n.Min == 0 {
			n.Min = temp
		}
		if temp > n.Max || n.Max == 0 {
			n.Max = temp
		}
		n.Sum += temp
		n.Count++
		return rest
	}

	if n.Children[cur] == nil {
		n.Children[cur] = &Node{Key: cur, Children: make([]*Node, 256)}
	}
	return n.Children[cur].Insert(value[1:])
}

func (n *Node) Merge(other *Node) {
	// since we chunked the file data then processed it in parallel, we need to merge the results
	// this takes the other node and merges it into this one creating any nodes as needed
	if other.Min < n.Min || n.Min == 0 {
		n.Min = other.Min
	}
	if other.Max > n.Max || n.Max == 0 {
		n.Max = other.Max
	}
	n.Sum += other.Sum
	n.Count += other.Count

	for i, child := range other.Children {
		if child != nil {
			if n.Children[i] == nil {
				n.Children[i] = &Node{Key: child.Key, Children: make([]*Node, 256)}
			}
			n.Children[i].Merge(child)
		}
	}
}

func (n *Node) Walk(label []byte) []string {
	// walk the trie, returning the results
	results := make([]string, 0)
	if n.Key != 0 {
		label = append(label, n.Key)
	}

	for _, child := range n.Children {
		if child != nil {
			results = append(results, child.Walk(label)...)
		}
	}

	if n.Count > 0 {
		min := math.Round(float64(n.Min)) / 10.0
		max := math.Round(float64(n.Max)) / 10.0
		mean := math.Round(float64(n.Sum)/float64(n.Count)) / 10.0
		results = append(results, fmt.Sprintf("%s=%.1f/%.1f/%.1f ", string(label), min, mean, max))
	}

	return results
}

func main() {
	start := time.Now()
	defer func() {
		fmt.Fprintf(os.Stderr, "took %s", time.Since(start))
	}()

	if cpuProfile {
		f, err := os.Create(fmt.Sprintf("./profiles/%s.pprof", time.Now().UTC().Format("2006-01-02T15:04:05Z07:00")))
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// open th file and read it in chunks
	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
	defer file.Close()

	wgChunkReaders := sync.WaitGroup{}
	wgResultAggregators := sync.WaitGroup{}
	chunks := make(chan []byte)
	results := make(chan *Node)
	total := make(chan *Node)

	// we want to read the file in chunks, and process each chunk in parallel
	for i := 0; i < runtime.NumCPU()-1; i++ {
		wgChunkReaders.Add(1)
		go func() {
			defer wgChunkReaders.Done()
			tree := &Node{Key: 0, Children: make([]*Node, 256)}
			for chunk := range chunks {
				for {
					chunk = tree.Insert(chunk)
					if len(chunk) == 0 {
						break
					}
				}
			}
			results <- tree
		}()
	}

	// we want to aggregate the results from the chunk readers
	wgResultAggregators.Add(1)
	go func() {
		defer wgResultAggregators.Done()
		tree := &Node{Key: 0, Children: make([]*Node, 256)}
		for result := range results {
			tree.Merge(result)
		}
		total <- tree
	}()

	// read the file in chunks
	for {
		buf := make([]byte, 256*1024*1024)
		num, err := file.Read(buf)
		if err != nil {
			break
		}
		// if the buffer is less than 256MB, send it all - we're at the end of the file
		if num < 256*1024*1024 {
			chunks <- buf[:num]
		} else {
			// find the last newline in the buffer, and send everything up to that point
			last := bytes.LastIndex(buf[:num], []byte("\n"))
			chunks <- buf[:last]
			// seek back to the start of the last line
			if last < num-1 {
				file.Seek(int64(last-num+1), 1)
			}
		}
	}
	// once the file is read, close the chunks channel and wait for the chunk readers to finish
	close(chunks)
	wgChunkReaders.Wait()

	// once the chunk readers are done, close the results channel and wait for the result aggregators to finish
	close(results)

	// get the merged results
	totalResult := <-total
	close(total)

	// walk the trie and print the results
	cities := totalResult.Walk([]byte{})
	fmt.Print("{ ")
	for i, city := range cities {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(city)
	}
	fmt.Print("}\n")
}

func extractValue(value []byte) (int, []byte) {
	// don't use strconv.Atoi because it's slow
	mult := 1
	result := 0
	sign := 1

	end := -1
	for i, c := range value {
		if c == '\n' {
			end = i
			break
		}
	}
	if end == -1 {
		end = len(value) - 1
	}
	for i := end; i >= 0; i-- {
		if value[i] == '.' {
			continue
		}
		if value[i] == '-' {
			sign = -1
			continue
		}
		result += int(value[i]-'0') * mult
		mult *= 10
	}
	return result * sign, value[end+1:]
}
