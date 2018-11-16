package kubernetes

import (
	"errors"
	"fmt"
	"sort"

	"github.com/hashicorp/terraform/backend"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	"github.com/hashicorp/terraform/terraform"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (b *Backend) States() ([]string, error) {
	states := []string{backend.DefaultStateName}

	secrets, err := b.k8sClient.CoreV1().Secrets(b.namespace).List(
		metav1.ListOptions{
			LabelSelector: terraStateLabel + "=true",
		},
	)
	if err != nil {
		return nil, err
	}

	for _, secret := range secrets.Items {
		if name, ok := parseSecretName(secret.Name); ok && name != backend.DefaultStateName {
			states = append(states, name)
		}
	}

	sort.Strings(states[1:])
	return states, nil
}

func (b *Backend) DeleteState(name string) error {
	if name == backend.DefaultStateName || name == "" {
		return fmt.Errorf("can't delete default state")
	}

	client, err := b.remoteClient(name)
	if err != nil {
		return err
	}

	return client.Delete()
}

func (b *Backend) State(name string) (state.State, error) {
	c, err := b.remoteClient(name)
	if err != nil {
		return nil, err
	}

	stateMgr := &remote.State{Client: c}

	// Grab the value
	if err := stateMgr.RefreshState(); err != nil {
		return nil, err
	}

	// If we have no state, we have to create an empty state
	if v := stateMgr.State(); v == nil {

		lockInfo := state.NewLockInfo()
		lockInfo.Operation = "init"
		lockID, err := stateMgr.Lock(lockInfo)
		if err != nil {
			return nil, err
		}

		// Local helper function so we can call it multiple places
		unlock := func(baseErr error) error {
			if err := stateMgr.Unlock(lockID); err != nil {
				const unlockErrMsg = `%v
				Additionally, unlocking the state file in Kubernetes failed:

				Error message: %q
				Lock ID (gen): %v
				Lock file Name: %v

				You may have to force-unlock this state in order to use it again.
				The Kubernetes backend acquires a lock during initialization to ensure
				the initial state file is created.`
				return fmt.Errorf(unlockErrMsg, baseErr, err.Error(), lockID, createLockName(c.workspace))
			}

			return baseErr
		}

		if err := stateMgr.WriteState(terraform.NewState()); err != nil {
			return nil, unlock(err)
		}
		if err := stateMgr.PersistState(); err != nil {
			return nil, unlock(err)
		}

		// Unlock, the state should now be initialized
		if err := unlock(nil); err != nil {
			return nil, err
		}

	}

	return stateMgr, nil
}

// get a remote client configured for this state
func (b *Backend) remoteClient(name string) (*RemoteClient, error) {
	if name == "" {
		return nil, errors.New("missing state name")
	}

	client := &RemoteClient{
		k8sClient: b.k8sClient,
		namespace: b.namespace,
		workspace: name,
	}

	return client, nil
}

func (b *Backend) client() *RemoteClient {
	return &RemoteClient{}
}

const errStateUnlock = `
Error unlocking k8s state. Lock ID: %s

Error: %s

You may have to force-unlock this state in order to use it again.
`
