package scaleset

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset/internal/testserver"
)

func acquirableTestClient(server *httptest.Server) *Client {
	return &Client{
		actionsServiceURL:                 server.URL,
		actionsServiceAdminToken:          "admin-token",
		actionsServiceAdminTokenExpiresAt: time.Now().Add(time.Hour),
		commonClient:                      commonClient{httpClient: server.Client(), userAgent: "crf-contract-test"},
	}
}

func TestGetAcquirableJobsHTTPContract(t *testing.T) {
	queued := "2026-08-21T06:30:00Z"
	workflowRef := "dinglebear-ai/soma/.github/workflows/ci.yml@refs/heads/main"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/_apis/runtime/runnerscalesets/74/acquirablejobs" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("api-version"); got != "6.0-preview" {
			t.Errorf("unexpected api-version: %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Errorf("unexpected authorization: %q", got)
		}
		fmt.Fprintf(w, `{"count":1,"value":[{"runnerRequestId":101,"ownerName":"dinglebear-ai","repositoryName":"soma","jobWorkflowRef":%q,"jobDisplayName":"unit","queueTime":%q}]}`, workflowRef, queued)
	}))
	defer server.Close()

	jobs, err := acquirableTestClient(server).GetAcquirableJobs(context.Background(), 74)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].RunnerRequestID != 101 || jobs[0].OwnerName != "dinglebear-ai" ||
		jobs[0].RepositoryName != "soma" || jobs[0].JobWorkflowRef != workflowRef || jobs[0].JobDisplayName != "unit" ||
		!jobs[0].QueueTime.Equal(time.Date(2026, 8, 21, 6, 30, 0, 0, time.UTC)) {
		t.Fatalf("metadata/time decoding changed: %#v", jobs)
	}
}

func TestGetAcquirableJobsEmptyErrorAndBodyFuses(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		wantEmpty bool
		wantError string
		wantTyped bool
	}{
		{name: "no-content", status: http.StatusNoContent, wantEmpty: true},
		{name: "typed-error", status: http.StatusConflict, body: `{"typeName":"AgentExistsException","message":"conflict"}`, wantError: "conflict", wantTyped: true},
		{name: "malformed", status: http.StatusOK, body: `{"count":1,"value":[`, wantError: "failed to decode acquirable jobs"},
		{name: "oversized", status: http.StatusOK, body: strings.Repeat(" ", maxAcquirableJobsResponseBytes+1), wantError: "exceeds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			jobs, err := acquirableTestClient(server).GetAcquirableJobs(context.Background(), 1)
			if tc.wantEmpty {
				if err != nil || len(jobs) != 0 {
					t.Fatalf("expected empty success: jobs=%v err=%v", jobs, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantTyped && !errors.Is(err, RunnerExistsError) {
				t.Fatalf("typed error was lost: %v", err)
			}
		})
	}
}

func TestGetAcquirableJobsHonorsCanceledAndDeadlineContexts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	client := acquirableTestClient(server)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.GetAcquirableJobs(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context was lost: %v", err)
	}
	deadline, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := client.GetAcquirableJobs(deadline, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline context was lost: %v", err)
	}
}

func TestGetAcquirableJobsDoesNotBlockConcurrentArrivals(t *testing.T) {
	firstArrived := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(firstArrived)
			select {
			case <-releaseFirst:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write([]byte(`{"count":0,"value":[]}`))
	}))
	defer server.Close()
	client := acquirableTestClient(server)

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.GetAcquirableJobs(context.Background(), 1)
		firstDone <- err
	}()
	<-firstArrived

	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := client.GetAcquirableJobs(short, 1)
		secondDone <- err
	}()
	var secondErr error
	select {
	case secondErr = <-secondDone:
	case <-short.Done():
		secondErr = short.Err()
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	if errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("concurrent arrival was blocked behind the first response: %v", secondErr)
	}
	if secondErr != nil {
		t.Fatalf("concurrent request failed: %v", secondErr)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}

func TestGetAcquirableJobsRejectsInvalidCounts(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "negative", body: `{"count":-1,"value":[]}`},
		{name: "mismatch", body: `{"count":2,"value":[{"runnerRequestId":101}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			if _, err := acquirableTestClient(server).GetAcquirableJobs(context.Background(), 1); err == nil ||
				!strings.Contains(err.Error(), "invalid acquirable jobs count") {
				t.Fatalf("invalid count was accepted: %v", err)
			}
		})
	}
}

func TestGetAcquirableJobsDeadlineWhileTokenRefreshIsBlocked(t *testing.T) {
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	server := testserver.New(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"count":0,"value":[]}`))
	}), testserver.WithRunnerRegistrationTokenHandler(func(w http.ResponseWriter, r *http.Request) {
		close(refreshStarted)
		select {
		case <-releaseRefresh:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"token":"registration-token"}`))
	}))
	client, err := newClient(testSystemInfo, server.ConfigURLForOrg("my-org"), actionsAuth{token: "token"})
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := client.GetAcquirableJobs(context.Background(), 1)
		firstDone <- err
	}()
	<-refreshStarted

	deadline, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := client.GetAcquirableJobs(deadline, 1)
		secondDone <- err
	}()
	var secondErr error
	select {
	case secondErr = <-secondDone:
	case <-deadline.Done():
		select {
		case secondErr = <-secondDone:
		case <-time.After(50 * time.Millisecond):
			secondErr = errors.New("GetAcquirableJobs did not return when its context expired")
		}
	}
	close(releaseRefresh)
	if err := <-firstDone; err != nil {
		t.Fatalf("refreshing request failed: %v", err)
	}
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Fatalf("short-deadline waiter returned %v, want context deadline exceeded", secondErr)
	}
}
