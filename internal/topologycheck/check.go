// Package topologycheck evaluates configured origin routes without IO.
package topologycheck

import (
	"fmt"
	"slices"
	"sort"
)

type Policy struct {
	Mode    string   `json:"mode"`
	Centers []string `json:"centers"`
	MaxHops uint32   `json:"maxHops"`
}

type Edge struct {
	ID            string `json:"id"`
	Source        string `json:"source"`
	Target        string `json:"target"`
	Policy        Policy `json:"policy"`
	ConfigVersion uint64 `json:"configVersion"`
}

type Path struct {
	Centers   []string `json:"centers"`
	Pipelines []string `json:"pipelines"`
}

type Warning struct {
	Kind     string `json:"kind"`
	Origin   string `json:"origin"`
	Target   string `json:"target"`
	Path     Path   `json:"path"`
	Previous *Path  `json:"previous,omitempty"`
}

type Result struct {
	Warnings   []Warning `json:"warnings"`
	Incomplete bool      `json:"incomplete"`
}

func (p Policy) allows(origin, local string, hops uint32) bool {
	if p.MaxHops > 0 && hops > p.MaxHops {
		return false
	}
	switch p.Mode {
	case "", "all":
		return true
	case "local_only":
		return origin == local
	case "include_origins":
		return slices.Contains(p.Centers, origin)
	case "exclude_origins":
		return !slices.Contains(p.Centers, origin)
	default:
		return false
	}
}

// Analyze expands each center once per origin, retaining the first path and a
// second witness on duplicate arrival. Cycles terminate at the original center.
func Analyze(centers []string, edges []Edge) (Result, error) {
	out := Result{Warnings: []Warning{}}
	if len(centers) > 1000 || len(edges) > 4000 {
		return out, fmt.Errorf("topology graph exceeds size limit")
	}
	nodes := slices.Clone(centers)
	sort.Strings(nodes)
	known := map[string]bool{}
	for _, node := range nodes {
		if node == "" || known[node] {
			return out, fmt.Errorf("invalid or duplicate topology center")
		}
		known[node] = true
	}
	ordered := slices.Clone(edges)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	bySource := map[string][]Edge{}
	ids := map[string]bool{}
	for _, edge := range ordered {
		if edge.ID == "" || ids[edge.ID] || !known[edge.Source] || !known[edge.Target] {
			return out, fmt.Errorf("invalid topology edge")
		}
		ids[edge.ID] = true
		switch edge.Policy.Mode {
		case "", "all", "local_only":
			if len(edge.Policy.Centers) != 0 {
				return out, fmt.Errorf("unexpected origin policy centers")
			}
		case "include_origins", "exclude_origins":
			if len(edge.Policy.Centers) == 0 {
				return out, fmt.Errorf("origin policy centers required")
			}
		default:
			return out, fmt.Errorf("invalid origin policy mode")
		}
		bySource[edge.Source] = append(bySource[edge.Source], edge)
	}
	for _, origin := range nodes {
		first := map[string]Path{origin: {Centers: []string{origin}, Pipelines: []string{}}}
		queue := []string{origin}
		for head := 0; head < len(queue); head++ {
			current := queue[head]
			path := first[current]
			for _, edge := range bySource[current] {
				// A source GTID proves original/current centers, so runtime hops
				// is 1 locally and 2 for foreign origins until history is stored.
				hops := uint32(2)
				if current == origin {
					hops = 1
				}
				if !edge.Policy.allows(origin, current, hops) {
					continue
				}
				next := Path{Centers: append(slices.Clone(path.Centers), edge.Target), Pipelines: append(slices.Clone(path.Pipelines), edge.ID)}
				old, seen := first[edge.Target]
				if !seen {
					first[edge.Target] = next
					queue = append(queue, edge.Target)
					continue
				}
				warning := Warning{Kind: "duplicate_arrival", Origin: origin, Target: edge.Target, Path: next, Previous: &old}
				if edge.Target == origin {
					warning.Kind = "loop"
					warning.Previous = nil
				}
				if len(out.Warnings) == 2000 {
					out.Incomplete = true
					return out, nil
				}
				out.Warnings = append(out.Warnings, warning)
			}
		}
	}
	return out, nil
}
