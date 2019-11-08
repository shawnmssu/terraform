package ufile

import (
	"errors"
	"fmt"
	"github.com/hashicorp/terraform/backend"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	"github.com/hashicorp/terraform/states"
	"path"
	"sort"
	"strings"
)

const (
	lockFileSuffix = ".tflock"
)

// get a remote client configured for this state
func (b *Backend) remoteClient(name string) (*remoteClient, error) {
	if name == "" {
		return nil, errors.New("missing state name")
	}

	client := &remoteClient{
		ufileClient: b.ufileClient,
		tagClient:   b.tagClient,
		bucketName:  b.bucketName,
		stateFile:   b.stateFile(name),
		lockFile:    b.lockFile(name),
	}

	return client, nil
}

func (b *Backend) Workspaces() ([]string, error) {
	wss := []string{backend.DefaultStateName}
	var limit = 20
	var marker string
	for {
		resp, err := b.ufileClient.PrefixFileList(b.prefix, marker, limit)
		if err != nil {
			return nil, fmt.Errorf("error on reading file list by prefix, %s", err)
		}

		if len(resp.DataSet) < 1 {
			break
		}

		for _, v := range resp.DataSet {
			if path.Join(b.prefix, b.keyName) == v.FileName {
				// filter the default workspace
				continue
			}
			parts := strings.Split(strings.TrimPrefix(v.FileName, b.prefix+"/"), "/")
			if len(parts) > 0 && parts[0] != "" {
				wss = append(wss, parts[0])
			}
		}

		if len(resp.DataSet) < limit {
			break
		}

		marker = resp.NextMarker
	}

	sort.Strings(wss[1:])
	return wss, nil
}

func (b *Backend) DeleteWorkspace(name string) error {
	if name == backend.DefaultStateName || name == "" {
		return fmt.Errorf("can't delete default state")
	}

	client, err := b.remoteClient(name)
	if err != nil {
		return err
	}

	return client.Delete()
}

func (b *Backend) StateMgr(name string) (state.State, error) {
	client, err := b.remoteClient(name)
	if err != nil {
		return nil, err
	}

	stateMgr := &remote.State{Client: client}
	// Check to see if this state already exists.
	// If we're trying to force-unlock a state, we can't take the lock before
	// fetching the state. If the state doesn't exist, we have to assume this
	// is a normal create operation, and take the lock at that point.
	//
	// If we need to force-unlock, but for some reason the state no longer
	// exists, the user will have to use aws tools to manually fix the
	// situation.
	existing, err := b.Workspaces()
	if err != nil {
		return nil, err
	}

	exists := false
	for _, s := range existing {
		if s == name {
			exists = true
			break
		}
	}

	// We need to create the object so it's listed by States.
	if !exists {
		// take a lock on this state while we write it
		lockInfo := state.NewLockInfo()
		lockInfo.Operation = "init"
		lockId, err := client.Lock(lockInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to lock ufile state: %s", err)
		}

		// Local helper function so we can call it multiple places
		lockUnlock := func(parent error) error {
			if err := stateMgr.Unlock(lockId); err != nil {
				return fmt.Errorf(strings.TrimSpace(errStateUnlock), lockId, err)
			}
			return parent
		}

		// Grab the value
		// This is to ensure that no one beat us to writing a state between
		// the `exists` check and taking the lock.
		if err := stateMgr.RefreshState(); err != nil {
			err = lockUnlock(err)
			return nil, err
		}

		// If we have no state, we have to create an empty state
		if v := stateMgr.State(); v == nil {
			if err := stateMgr.WriteState(states.NewState()); err != nil {
				err = lockUnlock(err)
				return nil, err
			}
			if err := stateMgr.PersistState(); err != nil {
				err = lockUnlock(err)
				return nil, err
			}
		}

		// Unlock, the state should now be initialized
		if err := lockUnlock(nil); err != nil {
			return nil, err
		}

	}

	return stateMgr, nil
}

func (b *Backend) stateFile(name string) string {
	if name == backend.DefaultStateName {
		//TODO:path.Join(b.prefix, b.keyName+stateFileSuffix)
		return path.Join(b.prefix, b.keyName)
	}

	return path.Join(b.prefix, name, b.keyName)
}

func (b *Backend) lockFile(name string) string {
	return b.stateFile(name) + lockFileSuffix
}

const errStateUnlock = `
Error unlocking UFile state. Lock ID: %s

Error: %s

You may have to force-unlock this state in order to use it again.
`
