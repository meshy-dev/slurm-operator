// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmcontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"k8s.io/utils/set"
	"sigs.k8s.io/controller-runtime/pkg/log"

	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/hostlist"
	slurmobject "github.com/SlinkyProject/slurm-client/pkg/object"
	slurmtypes "github.com/SlinkyProject/slurm-client/pkg/types"

	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	"github.com/SlinkyProject/slurm-operator/internal/clientmap"
	nodesetutils "github.com/SlinkyProject/slurm-operator/internal/controller/nodeset/utils"
	"github.com/SlinkyProject/slurm-operator/internal/utils/podinfo"
	"github.com/SlinkyProject/slurm-operator/internal/utils/timestore"
	slurmconditions "github.com/SlinkyProject/slurm-operator/pkg/conditions"
)

type SlurmControlInterface interface {
	// RefreshNodeCache forces the Node cache to be refreshed
	RefreshNodeCache(ctx context.Context, nodeset *slinkyv1beta1.NodeSet) error
	// UpdateNodeWithPodInfo handles updating the Node with its pod info
	UpdateNodeWithPodInfo(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) error
	// UpdateNodeTopology handles updating the Node with its topologySpec.
	UpdateNodeTopology(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod, topologySpec string) error
	// MakeNodeDrain handles adding the DRAIN state to the slurm node.
	MakeNodeDrain(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod, reason string, overrideReason bool) error
	// MakeNodeUndrain handles removing the DRAIN state from the slurm node.
	MakeNodeUndrain(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod, reason string) error
	// IsNodeDrain checks if the slurm node has the DRAIN state.
	IsNodeDrain(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error)
	// IsNodeDrained checks if the slurm node is drained.
	IsNodeDrained(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error)
	// IsNodeDrainedExceptExternalJobs checks if the slurm node has the DRAIN
	// state and the only jobs still holding resources on it are external jobs.
	IsNodeDrainedExceptExternalJobs(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error)
	// IsNodeDownForUnresponsive checks if the slurm node is unresponsive
	IsNodeDownForUnresponsive(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error)
	// IsNodeReasonOurs reports if the node reason was set by the operator.
	IsNodeReasonOurs(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error)
	// CalculateNodeStatus returns the current state of the registered slurm nodes.
	CalculateNodeStatus(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pods []*corev1.Pod) (SlurmNodeStatus, error)
	// GetNodeJobs returns, from one job list, each node's deadline calculated
	// from running jobs and the nodes whose only jobs are external jobs.
	GetNodeJobs(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pods []*corev1.Pod) (*NodeJobs, error)
	// GetNodesForPods returns a list of Slurm nodes associated with the NodeSet pods.
	GetNodesForPods(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pods []*corev1.Pod) ([]string, bool, error)
}

// ErrNodeInvalidReg is returned when a state-mutating operation is attempted on a
// node in INVALID_REG state, which Slurm rejects with "Invalid node state transition".
var ErrNodeInvalidReg = errors.New("NodeInvalidReg")

// realSlurmControl is the default implementation of SlurmControlInterface.
type realSlurmControl struct {
	clientMap *clientmap.ClientMap
}

// RefreshNodeCache implements SlurmControlInterface.
func (r *realSlurmControl) RefreshNodeCache(ctx context.Context, nodeset *slinkyv1beta1.NodeSet) error {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do RefreshNodeCache()")
		return nil
	}

	nodeList := &slurmtypes.V0044NodeList{}
	opts := &slurmclient.ListOptions{RefreshCache: true}
	if err := slurmClient.List(ctx, nodeList, opts); err != nil {
		return err
	}

	return nil
}

// UpdateNodeWithPodInfo implements SlurmControlInterface.
func (r *realSlurmControl) UpdateNodeWithPodInfo(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) error {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do UpdateNodeWithPodInfo()",
			"pod", klog.KObj(pod))
		return nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	podInfo := podinfo.PodInfo{
		Namespace: pod.GetNamespace(),
		PodName:   pod.GetName(),
		Node:      pod.Spec.NodeName,
	}
	podInfoOld := &podinfo.PodInfo{}
	_ = podinfo.ParseIntoPodInfo(slurmNode.Comment, podInfoOld)

	if podInfoOld.Equal(podInfo) {
		logger.V(3).Info("Node already contains podInfo, skipping update request",
			"node", slurmNode.GetKey(), "podInfo", podInfo)
		return nil
	}

	logger.Info("Update Slurm Node with Kubernetes Pod info",
		"Node", slurmNode.Name, "podInfo", podInfo)
	req := slurmapi.V0044UpdateNodeMsg{
		Comment: ptr.To(podInfo.ToString()),
	}
	if err := slurmClient.Update(ctx, slurmNode, req); err != nil {
		if !tolerateError(err) {
			return err
		}
	}

	if podInfoOld.Node != "" {
		logger.Info("Update Slurm Node state due to Kubernetes node migration", "Node", slurmNode.Name)
		req := slurmapi.V0044UpdateNodeMsg{
			State: ptr.To([]slurmapi.V0044UpdateNodeMsgState{slurmapi.V0044UpdateNodeMsgStateIDLE}),
		}
		if err := slurmClient.Update(ctx, slurmNode, req); err != nil {
			if tolerateError(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// UpdateNodeTopology implements SlurmControlInterface.
func (r *realSlurmControl) UpdateNodeTopology(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod, topologySpec string) error {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do UpdateNodeTopology()",
			"pod", klog.KObj(pod))
		return nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	nodeTopology := ptr.Deref(slurmNode.Topology, "")
	if apiequality.Semantic.DeepEqual(nodeTopology, topologySpec) {
		logger.V(3).Info("Node topologySpec is identical to request, skipping update request",
			"node", slurmNode.GetKey(), "topologySpec", nodeTopology)
		return nil
	}

	logger.Info("Update Slurm Node topologySpec", "Node", slurmNode.GetKey(), "topologySpec", topologySpec)
	req := slurmapi.V0044UpdateNodeMsg{
		TopologyStr: ptr.To(topologySpec),
	}
	if err := slurmClient.Update(ctx, slurmNode, req); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	return nil
}

const nodeReasonPrefix = "slurm-operator: "

// MakeNodeDrain implements SlurmControlInterface.
func (r *realSlurmControl) MakeNodeDrain(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod, reason string, overrideReason bool) error {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do MakeNodeDrain()",
			"pod", klog.KObj(pod))
		return nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	nodeReason := ptr.Deref(slurmNode.Reason, "")
	newReason := FormatNodeReason(reason)
	if !overrideReason && nodeReason != "" {
		newReason = nodeReason
	}

	// If Slurm node is already drained and the reasons match, no need to drain it again
	if slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateDRAIN) && nodeReason == newReason {
		logger.V(1).Info("Node is already drained, skipping drain request",
			"node", slurmNode.GetKey(), "nodeState", slurmNode.State, "nodeReason", nodeReason)
		return nil
	}

	if slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateINVALIDREG) {
		logger.V(1).Info("Node has INVALID_REG state, skipping drain request",
			"node", slurmNode.GetKey(), "nodeState", slurmNode.State)
		return ErrNodeInvalidReg
	}

	logger.V(1).Info("make slurm node drain",
		"pod", klog.KObj(pod))
	req := slurmapi.V0044UpdateNodeMsg{
		State:  ptr.To([]slurmapi.V0044UpdateNodeMsgState{slurmapi.V0044UpdateNodeMsgStateDRAIN}),
		Reason: ptr.To(newReason),
	}
	if err := slurmClient.Update(ctx, slurmNode, req); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	return nil
}

func FormatNodeReason(reason string) string {
	return nodeReasonPrefix + reason
}

// MakeNodeUndrain implements SlurmControlInterface.
func (r *realSlurmControl) MakeNodeUndrain(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod, reason string) error {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do MakeNodeUndrain()",
			"pod", klog.KObj(pod))
		return nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	if !slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateDRAIN) ||
		slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateUNDRAIN) {
		logger.V(1).Info("Node is already undrained, skipping undrain request",
			"node", slurmNode.GetKey(), "nodeState", slurmNode.State)
		return nil
	}

	if slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateINVALIDREG) {
		logger.V(1).Info("Node has INVALID_REG state, skipping undrain request",
			"node", slurmNode.GetKey(), "nodeState", slurmNode.State)
		return ErrNodeInvalidReg
	}

	// If the reason is not empty, prefix it with nodeReasonPrefix
	prefixedReason := ""
	if reason != "" {
		prefixedReason = FormatNodeReason(reason)
	}

	logger.V(1).Info("make slurm node undrain",
		"pod", klog.KObj(pod))
	req := slurmapi.V0044UpdateNodeMsg{
		State:  ptr.To([]slurmapi.V0044UpdateNodeMsgState{slurmapi.V0044UpdateNodeMsgStateUNDRAIN}),
		Reason: ptr.To(prefixedReason),
	}
	if err := slurmClient.Update(ctx, slurmNode, req); err != nil {
		if tolerateError(err) {
			return nil
		}
		return err
	}

	return nil
}

// IsNodeDrain implements SlurmControlInterface.
func (r *realSlurmControl) IsNodeDrain(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error) {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do IsNodeDrain()",
			"pod", klog.KObj(pod))
		return true, nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return true, nil
		}
		return false, err
	}

	isDrain := slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateDRAIN)
	return isDrain, nil
}

// IsNodeDrained implements SlurmControlInterface.
func (r *realSlurmControl) IsNodeDrained(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error) {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do IsNodeDrained()",
			"pod", klog.KObj(pod))
		return true, nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return true, nil
		}
		return false, err
	}

	// Drained is when a node has the DRAIN flag and is not doing any work (e.g. job step, prolog, epilog).
	// https://github.com/SchedMD/slurm/blob/slurm-25.05/src/common/slurm_protocol_defs.c#L3500
	isBusy := slurmNode.GetStateAsSet().HasAny(slurmapi.V0044NodeStateALLOCATED, slurmapi.V0044NodeStateMIXED, slurmapi.V0044NodeStateCOMPLETING)
	isDrain := slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateDRAIN) && !slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateUNDRAIN)
	isDrained := isDrain && !isBusy

	return isDrained, nil
}

// IsNodeDrainedExceptExternalJobs implements SlurmControlInterface.
func (r *realSlurmControl) IsNodeDrainedExceptExternalJobs(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error) {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do IsNodeDrainedExceptExternalJobs()",
			"pod", klog.KObj(pod))
		return false, nil
	}

	nodeName := nodesetutils.GetSlurmNodeName(pod)
	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodeName)
	if err := slurmClient.Get(ctx, key, slurmNode, &slurmclient.GetOptions{RefreshCache: true}); err != nil {
		if tolerateError(err) {
			return false, nil
		}
		return false, err
	}
	state := slurmNode.GetStateAsSet()
	if !state.Has(slurmapi.V0044NodeStateDRAIN) || state.Has(slurmapi.V0044NodeStateUNDRAIN) {
		return false, nil
	}

	// This decides whether to terminate the pod, so read jobs fresh.
	jobList := &slurmtypes.V0044JobInfoList{}
	if err := slurmClient.List(ctx, jobList, &slurmclient.ListOptions{RefreshCache: true}); err != nil {
		return false, err
	}
	externalOnly, err := externalJobsOnlyNodes(jobList.Items, set.New(nodeName))
	if err != nil {
		return false, err
	}
	_, ok := externalOnly[nodeName]
	return ok, nil
}

// IsNodeDownForUnresponsive implements SlurmControlInterface.
func (r *realSlurmControl) IsNodeDownForUnresponsive(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error) {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do IsNodeDrained()",
			"pod", klog.KObj(pod))
		return true, nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return true, nil
		}
		return false, err
	}

	// Slurm sets unresponsive nodes as `State=DOWN`, `Reason+="Not responding"`.
	// https://github.com/SchedMD/slurm/blob/slurm-25.05/src/slurmctld/ping_nodes.c#L243
	isDown := slurmNode.GetStateAsSet().Has(slurmapi.V0044NodeStateDOWN)
	reasonNotResponding := strings.Contains(ptr.Deref(slurmNode.Reason, ""), "Not responding")
	wasUnresponsive := isDown && reasonNotResponding

	return wasUnresponsive, nil
}

// IsNodeReasonOurs implements SlurmControlInterface.
func (r *realSlurmControl) IsNodeReasonOurs(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pod *corev1.Pod) (bool, error) {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do IsNodeReasonOurs()",
			"pod", klog.KObj(pod))
		return true, nil
	}

	slurmNode := &slurmtypes.V0044Node{}
	key := slurmobject.ObjectKey(nodesetutils.GetSlurmNodeName(pod))
	if err := slurmClient.Get(ctx, key, slurmNode); err != nil {
		if tolerateError(err) {
			return true, nil
		}
		return false, err
	}

	// The operator will always prefix the node reason.
	// External sources may not have a prefix or a different one.
	nodeReason := ptr.Deref(slurmNode.Reason, "")
	if nodeReason != "" && !strings.HasPrefix(nodeReason, nodeReasonPrefix) {
		return false, nil
	}

	return true, nil
}

type SlurmNodeStatus struct {
	Total int32

	// Base State
	Allocated int32
	Down      int32
	Error     int32
	Future    int32
	Idle      int32
	Mixed     int32
	Unknown   int32

	// Flag State
	Completing    int32
	Drain         int32
	Fail          int32
	Invalid       int32
	InvalidReg    int32
	Maintenance   int32
	NotResponding int32
	Undrain       int32

	// Per-node State as Conditions
	NodeStates map[string][]corev1.PodCondition
}

// CalculateNodeStatus implements SlurmControlInterface.
func (r *realSlurmControl) CalculateNodeStatus(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pods []*corev1.Pod) (SlurmNodeStatus, error) {
	logger := log.FromContext(ctx)
	status := SlurmNodeStatus{
		NodeStates: make(map[string][]corev1.PodCondition),
	}

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do CalculateNodeStatus()")
		return status, nil
	}

	nodeList := &slurmtypes.V0044NodeList{}
	if err := slurmClient.List(ctx, nodeList); err != nil {
		if tolerateError(err) {
			return status, nil
		}
		return status, err
	}

	podNodeNameSet := set.New[string]()
	for _, pod := range pods {
		podNodeName := nodesetutils.GetSlurmNodeName(pod)
		podNodeNameSet.Insert(podNodeName)
	}

	for _, node := range nodeList.Items {
		nodeName := ptr.Deref(node.Name, "")
		if !podNodeNameSet.Has(nodeName) {
			continue
		}
		status.Total++
		// Slurm Node Base States
		switch {
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateALLOCATED):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionAllocated))
			status.Allocated++
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateDOWN):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionDown))
			status.Down++
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateERROR):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionError))
			status.Error++
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateFUTURE):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionFuture))
			status.Future++
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateIDLE):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionIdle))
			status.Idle++
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateMIXED):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionMixed))
			status.Mixed++
		case node.GetStateAsSet().Has(slurmapi.V0044NodeStateUNKNOWN):
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionUnknown))
			status.Unknown++
		}
		// Slurm Node Flag State
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateCOMPLETING) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionCompleting))
			status.Completing++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateDRAIN) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionDrain))
			status.Drain++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateFAIL) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionFail))
			status.Fail++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateINVALID) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionInvalid))
			status.Invalid++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateINVALIDREG) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionInvalidReg))
			status.InvalidReg++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateMAINTENANCE) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionMaintenance))
			status.Maintenance++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateNOTRESPONDING) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionNotResponding))
			status.NotResponding++
		}
		if node.GetStateAsSet().Has(slurmapi.V0044NodeStateUNDRAIN) {
			status.NodeStates[nodeName] = append(status.NodeStates[nodeName],
				nodeState(node, slurmconditions.PodConditionUndrain))
			status.Undrain++
		}
	}

	return status, nil
}

// isJobHoldingResources reports whether a job currently holds resources on its
// nodes: running, suspended, or still completing.
func isJobHoldingResources(job *slurmtypes.V0044JobInfo) bool {
	return job.GetStateAsSet().HasAny(
		slurmapi.V0044JobInfoJobStateRUNNING,
		slurmapi.V0044JobInfoJobStateSUSPENDED,
		slurmapi.V0044JobInfoJobStateCOMPLETING,
	)
}

// isExternalJob reports whether the job has the EXTERNAL_JOB flag, i.e. its
// workload runs outside slurmd (e.g. a slurm-bridge pod).
func isExternalJob(job *slurmtypes.V0044JobInfo) bool {
	return slices.Contains(ptr.Deref(job.Flags, nil), slurmapi.V0044JobInfoFlagsEXTERNALJOB)
}

// externalJobsOnlyNodes returns, for each node in nodeNames that has at least
// one job holding resources and no non-external job holding resources, the
// number of external jobs on it.
func externalJobsOnlyNodes(jobs []slurmtypes.V0044JobInfo, nodeNames set.Set[string]) (map[string]int, error) {
	external := make(map[string]int)
	native := set.New[string]()
	for i := range jobs {
		job := &jobs[i]
		if !isJobHoldingResources(job) {
			continue
		}
		jobNodes, err := hostlist.Expand(ptr.Deref(job.Nodes, ""))
		if err != nil {
			return nil, fmt.Errorf("failed to expand node hostlist of job %d: %w", ptr.Deref(job.JobId, 0), err)
		}
		isExternal := isExternalJob(job)
		for _, nodeName := range jobNodes {
			if !nodeNames.Has(nodeName) {
				continue
			}
			if isExternal {
				external[nodeName]++
			} else {
				native.Insert(nodeName)
			}
		}
	}
	for nodeName := range native {
		delete(external, nodeName)
	}
	return external, nil
}

const infiniteDuration = time.Duration(math.MaxInt64)

// NodeJobs is what the NodeSet controller derives from the Slurm jobs on its
// nodes, computed from a single job list.
type NodeJobs struct {
	// Deadlines maps a Slurm node name to when its running jobs complete.
	Deadlines *timestore.TimeStore
	// ExternalJobsOnly maps a Slurm node name to its number of external jobs,
	// for nodes where at least one job and only external jobs hold resources.
	ExternalJobsOnly map[string]int
}

// GetNodeJobs implements SlurmControlInterface.
func (r *realSlurmControl) GetNodeJobs(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pods []*corev1.Pod) (*NodeJobs, error) {
	logger := log.FromContext(ctx)
	ts := timestore.NewTimeStore(timestore.Greater)
	nodeJobs := &NodeJobs{Deadlines: ts, ExternalJobsOnly: map[string]int{}}

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do GetNodeJobs()")
		return nodeJobs, nil
	}

	slurmNodeNamesSet := set.New[string]()
	for _, pod := range pods {
		slurmNodeName := nodesetutils.GetSlurmNodeName(pod)
		slurmNodeNamesSet.Insert(slurmNodeName)
	}

	jobList := &slurmtypes.V0044JobInfoList{}
	if err := slurmClient.List(ctx, jobList); err != nil {
		return nil, err
	}

	externalOnly, err := externalJobsOnlyNodes(jobList.Items, slurmNodeNamesSet)
	if err != nil {
		logger.Error(err, "failed to map jobs to nodes")
		return nil, err
	}
	nodeJobs.ExternalJobsOnly = externalOnly

	for _, job := range jobList.Items {
		if !job.GetStateAsSet().Has(slurmapi.V0044JobInfoJobStateRUNNING) {
			continue
		}
		slurmNodeNames, err := hostlist.Expand(ptr.Deref(job.Nodes, ""))
		if err != nil {
			logger.Error(err, "failed to expand job node hostlist",
				"job", ptr.Deref(job.JobId, 0))
			return nil, err
		}
		if !slurmNodeNamesSet.HasAny(slurmNodeNames...) {
			continue
		}

		// Get startTime, when the job was launched on the Slurm worker.
		startTime_NoVal := ptr.Deref(job.StartTime, slurmapi.V0044Uint64NoValStruct{})
		startTime := time.Unix(ptr.Deref(startTime_NoVal.Number, 0), 0)
		// Get the timeLimit, the wall time of the job.
		timeLimit_NoVal := ptr.Deref(job.TimeLimit, slurmapi.V0044Uint32NoValStruct{})
		timeLimit := time.Duration(ptr.Deref(timeLimit_NoVal.Number, 0)) * time.Minute
		if ptr.Deref(timeLimit_NoVal.Infinite, false) {
			timeLimit = infiniteDuration
		}

		// Push time/duration into the fancy map for each node allocated to the job.
		for _, slurmNodeName := range slurmNodeNames {
			ts.Push(slurmNodeName, startTime.Add(timeLimit))
		}
	}

	return nodeJobs, nil
}

// GetNodesForPods implements SlurmControlInterface.
func (r *realSlurmControl) GetNodesForPods(ctx context.Context, nodeset *slinkyv1beta1.NodeSet, pods []*corev1.Pod) ([]string, bool, error) {
	logger := log.FromContext(ctx)

	slurmClient := r.lookupClient(nodeset)
	if slurmClient == nil {
		logger.V(2).Info("no client for nodeset, cannot do GetNodesForPods()")
		return nil, false, nil
	}

	nodeList := &slurmtypes.V0044NodeList{}
	if err := slurmClient.List(ctx, nodeList); err != nil {
		return nil, true, err
	}

	// Expected Slurm nodes backed by NodeSet pods
	podNodeNameSet := set.New[string]()
	for _, pod := range pods {
		podNodeName := nodesetutils.GetSlurmNodeName(pod)
		podNodeNameSet.Insert(podNodeName)
	}

	// Actual Slurm nodes given NodeSet pods
	slurmNodeNames := []string{}
	for _, node := range nodeList.Items {
		nodeName := ptr.Deref(node.Name, "")
		if !podNodeNameSet.Has(nodeName) {
			continue
		}
		slurmNodeNames = append(slurmNodeNames, nodeName)
	}

	return slurmNodeNames, true, nil
}

func (r *realSlurmControl) lookupClient(nodeset *slinkyv1beta1.NodeSet) slurmclient.Client {
	key := ktypes.NamespacedName{
		Namespace: nodeset.Namespace,
		Name:      nodeset.Spec.ControllerRef.Name,
	}
	return r.clientMap.Get(key)
}

var _ SlurmControlInterface = &realSlurmControl{}

func NewSlurmControl(clientMap *clientmap.ClientMap) SlurmControlInterface {
	return &realSlurmControl{
		clientMap: clientMap,
	}
}

func tolerateError(err error) bool {
	if err == nil {
		return true
	}
	errText := err.Error()
	if errText == http.StatusText(http.StatusNotFound) ||
		errText == http.StatusText(http.StatusNoContent) {
		return true
	}
	return false
}

// Translate a Slurm node state to a plaintext state with a reason
// and a flag to indicate if it is a base state or a flag state.
func nodeState(node slurmtypes.V0044Node, condType corev1.PodConditionType) corev1.PodCondition {
	return corev1.PodCondition{
		Type:    condType,
		Status:  corev1.ConditionTrue,
		Message: ptr.Deref(node.Reason, ""),
	}
}
