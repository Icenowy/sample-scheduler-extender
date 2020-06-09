package controller

import (
	"log"
	"strings"

	"k8s.io/api/core/v1"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
)

const (
	AvxPred        = "AVX"
	AvxPredFailMsg = "Sorry, you are not powerful enough, buy a newer processor"
)

var predicatesFuncs = map[string]FitPredicate{
	AvxPred: AvxPredicate,
}

type FitPredicate func(pod *v1.Pod, node v1.Node) (bool, []string, error)

var predicatesSorted = []string{AvxPred}

// filter 根据扩展程序定义的预选规则来过滤节点
// it's webhooked to pkg/scheduler/core/generic_scheduler.go#findNodesThatFit()
func filter(args schedulerapi.ExtenderArgs) *schedulerapi.ExtenderFilterResult {
	var filteredNodes []v1.Node
	failedNodes := make(schedulerapi.FailedNodesMap)
	pod := args.Pod
	for _, node := range args.Nodes.Items {
		fits, failReasons, _ := podFitsOnNode(pod, node)
		if fits {
			filteredNodes = append(filteredNodes, node)
		} else {
			failedNodes[node.Name] = strings.Join(failReasons, ",")
		}
	}

	result := schedulerapi.ExtenderFilterResult{
		Nodes: &v1.NodeList{
			Items: filteredNodes,
		},
		FailedNodes: failedNodes,
		Error:       "",
	}

	return &result
}

func podFitsOnNode(pod *v1.Pod, node v1.Node) (bool, []string, error) {
	fits := true
	var failReasons []string
	for _, predicateKey := range predicatesSorted {
		fit, failures, err := predicatesFuncs[predicateKey](pod, node)
		if err != nil {
			return false, nil, err
		}
		fits = fits && fit
		failReasons = append(failReasons, failures...)
	}
	return fits, failReasons, nil
}

func AvxPredicate(pod *v1.Pod, node v1.Node) (bool, []string, error) {
	if strings.Contains(pod.Name, "-avx") {
		if strings.Contains(node.Name, "p1620") {
			// This is a Core 2, cannot do AVX
			log.Printf("node %v is not powerful enough for pod %v/%v\n", node.Name, pod.Name, pod.Namespace)
			return false, []string{AvxPredFailMsg}, nil
		} else {
			log.Printf("node %v is powerful enough for pod %v/%v\n", node.Name, pod.Name, pod.Namespace)
		}
	} else {
		log.Printf("pod %v/%v doesn't need power, so node %v can go\n", pod.Name, pod.Namespace, node.Name)
	}
	return true, nil, nil
}
