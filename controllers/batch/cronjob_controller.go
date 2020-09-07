/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	batchv1alpha1 "cronjob/apis/batch/v1alpha1"
)

const (
	jobOwnerKey             = ".metadata.controller"
	scheduledTimeAnnotation = "mesh.jly.io/scheduled-at"
)

var (
	// JobActive job condition type -- active
	JobActive  batchv1.JobConditionType
	APIVersion = batchv1alpha1.GroupVersion.String()
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=batch.operator.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.operator.io,resources=cronjobs/status,verbs=get;update;patch

func (r *CronJobReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("cronjob", req.NamespacedName)

	// your logic here
	var cronJob batchv1alpha1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "unable to fetch cronjob")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var childJobs batchv1.JobList
	matchingFields := client.MatchingFields{jobOwnerKey: req.Name}
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), matchingFields); err != nil {
		log.Error(err, "unable to list child jobs")
		return ctrl.Result{}, err
	}

	var mostRecentTime *time.Time
	jobsMap := map[batchv1.JobConditionType][]*batchv1.Job{
		JobActive:           make([]*batchv1.Job, 0, len(childJobs.Items)),
		batchv1.JobFailed:   make([]*batchv1.Job, 0, len(childJobs.Items)),
		batchv1.JobComplete: make([]*batchv1.Job, 0, len(childJobs.Items)),
	}
	for i := 0; i < len(childJobs.Items); i++ {
		job := &childJobs.Items[i]

		_, condType := isJobFinished(job)
		jobsMap[condType] = append(jobsMap[condType], job)

		scheduledTime, err := getJobScheduledTime(job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job")
			continue
		}
		if scheduledTime != nil {
			if mostRecentTime == nil || mostRecentTime.Before(*scheduledTime) {
				mostRecentTime = scheduledTime
			}
		}
	}

	if mostRecentTime == nil {
		cronJob.Status.LastScheduleTime = nil
	} else {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	}
	cronJob.Status.Active = nil
	for _, job := range jobsMap[JobActive] {
		jobRef, err := reference.GetReference(r.Scheme, job)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", job)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	log.V(1).Info(
		"job count",
		"active jobs", len(jobsMap[JobActive]),
		"successful jobs", len(jobsMap[batchv1.JobComplete]),
		"failed jobs", len(jobsMap[batchv1.JobFailed]))

	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		failedJobs := jobsMap[batchv1.JobFailed]
		failedJobHistoryLimit := *cronJob.Spec.FailedJobsHistoryLimit

		sort.Slice(failedJobs, func(i, j int) bool {
			istart := failedJobs[i].Status.StartTime
			jstart := failedJobs[j].Status.StartTime
			if istart == nil {
				return jstart != nil
			}
			return istart.Before(jstart)
		})

		for i := 0; i < len(failedJobs); i++ {
			if int32(i) >= int32(len(failedJobs))-failedJobHistoryLimit {
				break
			}
			policy := client.PropagationPolicy(metav1.DeletePropagationBackground)
			if err := r.Delete(ctx, failedJobs[i], policy); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", failedJobs[i])
			} else {
				log.V(0).Info("delete old failed job", "job", failedJobs[i])
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		successfulJobs := jobsMap[batchv1.JobComplete]
		successfulJobHistoryLimit := *cronJob.Spec.SuccessfulJobsHistoryLimit

		sort.Slice(successfulJobs, func(i, j int) bool {
			istart := successfulJobs[i].Status.StartTime
			jstart := successfulJobs[j].Status.StartTime
			if istart == nil {
				return jstart != nil
			}
			return istart.Before(jstart)
		})
		for i := 0; i < len(successfulJobs); i++ {
			if int32(i) >= int32(len(successfulJobs))-successfulJobHistoryLimit {
				break
			}
			policy := client.PropagationPolicy(metav1.DeletePropagationBackground)
			if err := r.Delete(ctx, successfulJobs[i], policy); err != nil {
				log.Error(err, "unable to delete old successful job", "job", successfulJobs[i])
			} else {
				log.V(0).Info("elete old successful job", "job", successfulJobs[i])
			}
		}
	}

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspend, skipping")
		return ctrl.Result{}, nil
	}

	missedRun, nextRun, err := getNextSchedule(&cronJob, time.Now())
	if err != nil {
		log.Error(err, "unable to figure out CronJob schedule")
		return ctrl.Result{}, err
	}

	scheduleResult := ctrl.Result{RequeueAfter: nextRun.Sub(time.Now())}
	log = log.WithValues("now", time.Now(), "next run", nextRun)

	if missedRun.IsZero() {
		log.V(1).Info("no upcoming schedule times, sleeping until next")
		return scheduleResult, nil
	}

	log = log.WithValues("current run", missedRun)

	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		startingDeadlineSeconds := *cronJob.Spec.StartingDeadlineSeconds
		tooLate = missedRun.Add(time.Duration(startingDeadlineSeconds) * time.Second).Before(time.Now())
	}
	if tooLate {
		log.V(1).Info("missed starting deadline for last run, sleeping till next")
		return scheduleResult, nil
	}

	if cronJob.Spec.ConcurrentPolicy == batchv1alpha1.ReplaceConcurrent {
		activeJobs := jobsMap[JobActive]
		for i := 0; i < len(activeJobs); i++ {
			policy := client.PropagationPolicy(metav1.DeletePropagationBackground)
			if err := r.Delete(ctx, activeJobs[i], policy); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete active job", "job", activeJobs[i])
				return ctrl.Result{}, err
			}
		}
	}

	job := constructJob(&cronJob, missedRun)
	if err := ctrl.SetControllerReference(&cronJob, job, r.Scheme); err != nil {
		log.Error(err, "unable to set controller reference")
		return scheduleResult, err
	}

	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create job")
		return ctrl.Result{}, err
	}

	log.V(1).Info("created Job for CronJob run", "job", job)

	return ctrl.Result{}, nil
}

func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(&batchv1.Job{}, jobOwnerKey, indexerFunc)
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1alpha1.CronJob{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func isJobFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, cond := range job.Status.Conditions {
		matchType := cond.Type == batchv1.JobComplete || cond.Type == batchv1.JobFailed
		matchStatus := cond.Status == corev1.ConditionTrue
		if matchType && matchStatus {
			return true, cond.Type
		}
	}
	return false, JobActive
}

func getJobScheduledTime(job *batchv1.Job) (*time.Time, error) {
	rawTime := job.Annotations[scheduledTimeAnnotation]
	if rawTime == "" {
		return nil, nil
	}
	objTime, err := time.Parse(time.RFC3339, rawTime)
	if err != nil {
		return nil, err
	}
	return &objTime, nil
}

func getNextSchedule(cronJob *batchv1alpha1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
	schedule, err := cron.ParseStandard(cronJob.Spec.Schedule)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("Unparseable schedule %q: %v", cronJob.Spec.Schedule, err)
	}

	var earliestTime time.Time
	if cronJob.Status.LastScheduleTime != nil {
		earliestTime = cronJob.Status.LastScheduleTime.Time
	} else {
		earliestTime = cronJob.CreationTimestamp.Time
	}
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		startingDeadlineSeconds := *cronJob.Spec.StartingDeadlineSeconds
		schedulingDeadline := now.Add(-time.Second * time.Duration(startingDeadlineSeconds))
		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}
	if earliestTime.After(now) {
		return time.Time{}, schedule.Next(now), nil
	}

	starts := 0
	for t := schedule.Next(earliestTime); !t.After(now); t = schedule.Next(t) {
		lastMissed = t
		starts++
		if starts > 100 {
			return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times (>100). Set or decrease .spec.startDeadlineSeconds or check clock skew")
		}
	}
	return lastMissed, schedule.Next(now), nil
}

func constructJob(cronJob *batchv1alpha1.CronJob, scheduledTime time.Time) *batchv1.Job {
	name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cronJob.Namespace,
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}
	for k, v := range cronJob.Spec.JobTemplate.Labels {
		job.Labels[k] = v
	}
	for k, v := range cronJob.Spec.JobTemplate.Annotations {
		job.Annotations[k] = v
	}
	job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
	return job
}

func indexerFunc(obj runtime.Object) []string {
	job := obj.(*batchv1.Job)
	owner := metav1.GetControllerOf(job)
	if owner == nil {
		return nil
	}
	if owner.APIVersion != APIVersion || owner.Kind != "Cronjob" {
		return nil
	}
	return []string{owner.Name}
}
