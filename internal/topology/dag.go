package topology

import (
	"fmt"
	"strings"
	"sync"

	"streamengine/internal/model"
)

// Edge connects two operators in the topology DAG.
type Edge struct {
	From string
	To   string
}

// DAG tracks operator dependencies and validates that the topology has no
// cycles before it is allowed to resume from a pause.
type DAG struct {
	mu    sync.Mutex
	edges []Edge
}

// NewDAG creates an empty operator graph.
func NewDAG() *DAG {
	return &DAG{}
}

// Connect adds a directed edge between two operator names.
func (d *DAG) Connect(from string, to string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.edges = append(d.edges, Edge{From: from, To: to})
}

// Validate performs a depth-first cycle check over the current edges.
func (d *DAG) Validate() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	adj := make(map[string][]string)
	for _, edge := range d.edges {
		adj[edge.From] = append(adj[edge.From], edge.To)
	}
	state := make(map[string]int)
	var visit func(node string) error
	visit = func(node string) error {
		switch state[node] {
		case 1:
			return fmt.Errorf("cycle detected at operator %s", node)
		case 2:
			return nil
		}
		state[node] = 1
		for _, next := range adj[node] {
			if err := visit(next); err != nil {
				return err
			}
		}
		state[node] = 2
		return nil
	}
	for node := range adj {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

// Describe renders the DAG as a text diagram for the monitoring page.
func (d *DAG) Describe() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	parts := make([]string, 0, len(d.edges))
	for _, edge := range d.edges {
		parts = append(parts, edge.From+" -> "+edge.To)
	}
	return strings.Join(parts, "; ")
}

// TopologyNode is a display helper binding an operator to its DAG position.
func TopologyNode(instance *model.OperatorInstance, dag *DAG) map[string]string {
	return map[string]string{
		"name":  instance.Spec.Name,
		"state": instance.State.String(),
		"dag":   dag.Describe(),
	}
}
