// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package conditions

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestIsConditionTrue(t *testing.T) {
	type args struct {
		status   *corev1.PodStatus
		condType corev1.PodConditionType
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Idle",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionIdle,
							Status: corev1.ConditionTrue,
						},
					},
				},
				condType: PodConditionIdle,
			},
			want: true,
		},
		{
			name: "Allocated not Idle",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionIdle,
							Status: corev1.ConditionFalse,
						},
					},
				},
				condType: PodConditionAllocated,
			},
			want: false,
		},
		{
			name: "Idle set to false",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionIdle,
							Status: corev1.ConditionFalse,
						},
					},
				},
				condType: PodConditionIdle,
			},
			want: false,
		},
		{
			name: "Drain set, multiple conditions",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionIdle,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
					},
				},
				condType: PodConditionDrain,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsConditionTrue(tt.args.status, tt.args.condType); got != tt.want {
				t.Errorf("IsCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNodeDrained(t *testing.T) {
	type args struct {
		status *corev1.PodStatus
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Node is drained (idle)",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionIdle,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node is drained (down)",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionDown,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node is not drained (allocated)",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionAllocated,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNodeDrained(tt.args.status); got != tt.want {
				t.Errorf("IsNodeDrained() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNodeDraining(t *testing.T) {
	type args struct {
		status *corev1.PodStatus
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Node is draining (allocated)",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionAllocated,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node is draining (mixed)",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionMixed,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node is not draining (idle)",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionIdle,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNodeDraining(tt.args.status); got != tt.want {
				t.Errorf("IsNodeDraining() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsSlurmNodeDrain(t *testing.T) {
	type args struct {
		status *corev1.PodStatus
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Node has drain state",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node does not have drain state",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{},
				},
			},
			want: false,
		},
		{
			name: "Node has undrain state",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionUndrain,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNodeDrain(tt.args.status); got != tt.want {
				t.Errorf("IsSlurmNodeDrain() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAreJobsRunning(t *testing.T) {
	type args struct {
		status *corev1.PodStatus
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Node has jobs running",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionDrain,
							Status: corev1.ConditionTrue,
						},
						{
							Type:   PodConditionAllocated,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node has jobs running",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionMixed,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Node is completing",
			args: args{
				status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:   PodConditionCompleting,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNodeBusy(tt.args.status); got != tt.want {
				t.Errorf("AreJobsRunning() = %v, want %v", got, tt.want)
			}
		})
	}
}
