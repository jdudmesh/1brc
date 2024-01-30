// main.go
package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"time"
)

type Node struct {
	Key      byte
	Children []*Node
	Min      int
	Max      int
	Sum      int
	Count    int
}

func (n *Node) Insert(value []byte) {
	cur := value[0]
	if cur == ';' {
		temp := extractValue(value[1:])
		if temp < n.Min || n.Min == 0 {
			n.Min = temp
		}
		if temp > n.Max || n.Max == 0 {
			n.Max = temp
		}
		n.Sum += temp
		n.Count++
		return
	}

	if n.Children[cur] == nil {
		n.Children[cur] = &Node{Key: cur, Children: make([]*Node, 256)}
	}
	n.Children[cur].Insert(value[1:])
}

func (n *Node) Walk(label []byte) []string {
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

	tree := &Node{Key: 0, Children: make([]*Node, 256)}

	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if line[0] == '#' {
			continue
		}
		tree.Insert(line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	results := tree.Walk([]byte{})
	fmt.Print("{ ")
	for i, result := range results {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(result)
	}
	fmt.Print("}\n")
}

func extractValue(value []byte) int {
	mult := 1
	result := 0
	sign := 1
	for i := len(value) - 1; i >= 0; i-- {
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
	return result * sign
}
