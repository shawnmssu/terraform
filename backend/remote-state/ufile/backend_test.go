package ufile

import (
	"fmt"
	"github.com/hashicorp/terraform/state/remote"
	"github.com/hashicorp/terraform/states"
	"github.com/ucloud/ucloud-sdk-go/services/ufile"
	"github.com/ucloud/ucloud-sdk-go/ucloud"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/terraform/backend"
	"github.com/hashicorp/terraform/configs/hcl2shim"
)

// verify that we are doing ACC tests or the UFile tests specifically
func testACC(t *testing.T) {
	skip := os.Getenv("TF_ACC") == "" && os.Getenv("TF_UFile_TEST") == ""
	if skip {
		t.Log("ufile backend tests require setting TF_ACC or TF_UFile_TEST")
		t.Skip()
	}
	if os.Getenv("UCloud_DEFAULT_REGION") == "" {
		os.Setenv("UCloud_DEFAULT_REGION", "cn-bj2")
	}
}

func TestBackend_impl(t *testing.T) {
	var _ backend.Backend = new(Backend)
}

func TestBackendConfig(t *testing.T) {
	testACC(t)
	config := map[string]interface{}{
		"region": "cn-bj2",
		"bucket": "tf-test",
		"key":    "state",
	}

	b := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(config)).(*Backend)

	if b.bucketName != "tf-test" {
		t.Fatalf("Incorrect bucketName was populated")
	}
	if b.keyName != "state" {
		t.Fatalf("Incorrect keyName was populated")
	}

	credentials := b.tagClient.Client.GetCredential()
	if credentials.PrivateKey == "" {
		t.Fatalf("No Private Key was populated")
	}
	if credentials.PrivateKey == "" {
		t.Fatalf("No Private Key was populated")
	}
}

func TestBackendConfig_invalidKey(t *testing.T) {
	testACC(t)
	cfg := hcl2shim.HCL2ValueFromConfigValue(map[string]interface{}{
		"region": "cn-bj2",
		"bucket": "tf-test",
		"key":    "/leading-slash",
	})

	_, diags := New().PrepareConfig(cfg)
	if !diags.HasErrors() {
		t.Fatal("expected config validation error")
	}
}

func TestBackend(t *testing.T) {
	testACC(t)

	bucketName := fmt.Sprintf("terraform-remote-ufile-test-%x", time.Now().Unix())
	keyName := "testState"

	b := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"bucket": bucketName,
		"key":    keyName,
	})).(*Backend)

	createUFileBucket(t, b.ufileBucketClient, bucketName)
	defer deleteUFileBucket(t, b.ufileBucketClient, b.ufileClient, bucketName)

	backend.TestBackendStates(t, b)
}

func TestBackendLocked(t *testing.T) {
	testACC(t)

	bucketName := fmt.Sprintf("terraform-remote-ufile-test-%x", time.Now().Unix())
	keyName := "test/state"

	b1 := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"bucket": bucketName,
		"key":    keyName,
	})).(*Backend)

	b2 := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"bucket": bucketName,
		"key":    keyName,
	})).(*Backend)

	createUFileBucket(t, b1.ufileBucketClient, bucketName)
	defer deleteUFileBucket(t, b1.ufileBucketClient, b1.ufileClient, bucketName)

	backend.TestBackendStateLocks(t, b1, b2)
	backend.TestBackendStateForceUnlock(t, b1, b2)
}

//add some extra junk in UFile to try and confuse the env listing.
//TODO:test failed
func TestBackendExtraPaths(t *testing.T) {
	testACC(t)
	bucketName := fmt.Sprintf("terraform-remote-ufile-test-%x", time.Now().Unix())
	keyName := "test/state/tfstate"

	b := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"bucket": bucketName,
		"key":    keyName,
	})).(*Backend)

	createUFileBucket(t, b.ufileBucketClient, bucketName)
	defer deleteUFileBucket(t, b.ufileBucketClient, b.ufileClient, bucketName)

	// put multiple states in old env paths.
	s1 := states.NewState()
	s2 := states.NewState()

	// remoteClient to Put things in various paths
	client := &remoteClient{
		ufileClient: b.ufileClient,
		tagClient:   b.tagClient,
		bucketName:  b.bucketName,
		stateFile:   b.stateFile("s1"),
		lockFile:    b.lockFile("s1"),
	}

	stateMgr := &remote.State{Client: client}
	stateMgr.WriteState(s1)
	if err := stateMgr.PersistState(); err != nil {
		t.Fatal(err)
	}

	client.stateFile = b.stateFile("s2")
	client.lockFile = client.stateFile + lockFileSuffix
	stateMgr.Client = client
	stateMgr.WriteState(s2)
	if err := stateMgr.PersistState(); err != nil {
		t.Fatal(err)
	}

	s2Lineage := stateMgr.StateSnapshotMeta().Lineage

	if err := checkStateList(b, []string{"default", "s1", "s2"}); err != nil {
		t.Fatal(err)
	}

	// add state with the wrong key for an existing env
	client.stateFile = b.prefix + "/s2/notTestState"
	client.lockFile = client.stateFile + lockFileSuffix
	stateMgr.WriteState(states.NewState())
	if err := stateMgr.PersistState(); err != nil {
		t.Fatal(err)
	}
	if err := checkStateList(b, []string{"default", "s1", "s2"}); err != nil {
		t.Fatal(err)
	}

	// remove the state with extra subkey
	if err := client.Delete(); err != nil {
		t.Fatal(err)
	}

	// delete the real workspace
	if err := b.DeleteWorkspace("s2"); err != nil {
		t.Fatal(err)
	}

	if err := checkStateList(b, []string{"default", "s1"}); err != nil {
		t.Fatal(err)
	}

	// fetch that state again, which should produce a new lineage
	s2Mgr, err := b.StateMgr("s2")
	if err != nil {
		t.Fatal(err)
	}
	if err := s2Mgr.RefreshState(); err != nil {
		t.Fatal(err)
	}

	if s2Mgr.(*remote.State).StateSnapshotMeta().Lineage == s2Lineage {
		t.Fatal("state s2 was not deleted")
	}
	s2 = s2Mgr.State()
	s2Lineage = stateMgr.StateSnapshotMeta().Lineage

	// add a state with a key that matches an existing environment dir name
	client.stateFile = b.prefix + "/s2/"
	stateMgr.WriteState(states.NewState())
	if err := stateMgr.PersistState(); err != nil {
		t.Fatal(err)
	}

	// make sure s2 is OK
	s2Mgr, err = b.StateMgr("s2")
	if err != nil {
		t.Fatal(err)
	}
	if err := s2Mgr.RefreshState(); err != nil {
		t.Fatal(err)
	}

	if stateMgr.StateSnapshotMeta().Lineage != s2Lineage {
		t.Fatal("we got the wrong state for s2")
	}

	if err := checkStateList(b, []string{"default", "s1", "s2"}); err != nil {
		t.Fatal(err)
	}
}

// ensure we can separate the workspace prefix when it also matches the prefix
// of the workspace name itself.
func TestBackendPrefixInWorkspace(t *testing.T) {
	testACC(t)
	bucketName := fmt.Sprintf("terraform-remote-ufile-test-%x", time.Now().Unix())

	b := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"bucket": bucketName,
		"key":    "test-env.tfstate",
		"prefix": "env",
	})).(*Backend)

	createUFileBucket(t, b.ufileBucketClient, bucketName)
	defer deleteUFileBucket(t, b.ufileBucketClient, b.ufileClient, bucketName)

	// get a state that contains the prefix as a substring
	sMgr, err := b.StateMgr("env-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sMgr.RefreshState(); err != nil {
		t.Fatal(err)
	}

	if err := checkStateList(b, []string{"default", "env-1"}); err != nil {
		t.Fatal(err)
	}
}

func checkStateList(b backend.Backend, expected []string) error {
	states, err := b.Workspaces()
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(states, expected) {
		return fmt.Errorf("incorrect states listed: %q", states)
	}
	return nil
}

func createUFileBucket(t *testing.T, ufileBucketClient *ufile.UFileClient, bucketName string) {
	req := ufileBucketClient.NewCreateBucketRequest()
	req.BucketName = ucloud.String(bucketName)

	// Be clear about what we're doing in case the user needs to clean
	// this up later.
	t.Logf("creating UFile bucket %s", bucketName)
	_, err := ufileBucketClient.CreateBucket(req)
	if err != nil {
		t.Fatal("failed to create test UFile bucket:", err)
	}
}

func deleteUFileBucket(t *testing.T, ufileBucketClient *ufile.UFileClient, ufileClient *ufsdk.UFileRequest, bucketName string) {
	warning := "WARNING: Failed to delete the test UFile bucket. It may have been left in your UCloud account and may incur storage charges. (error was %s)"

	// first we have to get rid of the env objects, or we can't delete the bucket
	var limit = 20
	var marker string
	for {
		resp, err := ufileClient.PrefixFileList("", marker, limit)
		if err != nil {
			t.Logf(warning, err)
			return
		}

		if len(resp.DataSet) < 1 {
			break
		}

		for _, v := range resp.DataSet {
			if err := ufileClient.DeleteFile(v.FileName); err != nil {
				// this will need cleanup no matter what, so just warn and exit
				t.Logf(warning, err)
				return
			}
		}

		if len(resp.DataSet) < limit {
			break
		}

		marker = resp.NextMarker
	}
	req := ufileBucketClient.NewDeleteBucketRequest()
	req.BucketName = ucloud.String(bucketName)
	if _, err := ufileBucketClient.DeleteBucket(req); err != nil {
		t.Logf(warning, err)
		return
	}

	return
}
