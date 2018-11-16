package kubernetes

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes"
)

const (
	terraState      = "terrastate-"
	terraLock       = "terralock-"
	terraStateLabel = "terrastate"
	terraLockLabel  = "tslock"
)

type RemoteClient struct {
	k8sClient kubernetes.Interface
	namespace string
	workspace string
}

func (c *RemoteClient) Get() (payload *remote.Payload, err error) {
	sName := createSecretName(c.workspace)
	secret, err := c.k8sClient.CoreV1().Secrets(c.namespace).Get(sName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	state, err := uncompressState(secret.Data[sName])
	if err != nil {
		return nil, err
	}

	md5 := md5.Sum(state)

	p := &remote.Payload{
		Data: state,
		MD5:  md5[:],
	}
	return p, nil
}

func (c *RemoteClient) Put(data []byte) error {
	sName := createSecretName(c.workspace)

	payload, err := compressState(data)
	if err != nil {
		return err
	}

	sData := make(map[string][]byte)
	sData[sName] = payload

	label := make(map[string]string)
	label[terraStateLabel] = "true"
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sName,
			Namespace: c.namespace,
			Labels:    label,
		},
		Data: sData,
	}

	_, err = c.k8sClient.CoreV1().Secrets(c.namespace).Create(&secret)
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			_, err = c.k8sClient.CoreV1().Secrets(c.namespace).Update(&secret)
		}
	}

	return err
}

// Delete the state secret
func (c *RemoteClient) Delete() error {
	sName := createSecretName(c.workspace)

	err := c.deleteSecret(sName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (c *RemoteClient) Lock(info *state.LockInfo) (string, error) {
	lName := createLockName(c.workspace)

	sData := make(map[string]string)
	sData["info"] = string(info.Marshal())

	label := make(map[string]string)
	label[terraLockLabel] = "true"
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      lName,
			Namespace: c.namespace,
			Labels:    label,
		},
		StringData: sData,
	}

	_, err := c.k8sClient.CoreV1().Secrets(c.namespace).Create(&secret)
	if err != nil {
		// The lock already exists, get info on the lock to pass back
		if k8serrors.IsAlreadyExists(err) {
			lockInfo, infoErr := c.getLockInfo(lName)
			if infoErr != nil {
				return "", multierror.Append(err, infoErr)
			}

			lockErr := &state.LockError{
				Err:  err,
				Info: lockInfo,
			}
			return "", lockErr
		}
		return "", err
	}

	return info.ID, err
}

func (c *RemoteClient) Unlock(id string) error {
	lName := createLockName(c.workspace)

	lockErr := &state.LockError{}

	lockInfo, err := c.getLockInfo(lName)
	if err != nil {
		// The lock doesn't exist if secret isn't found
		if k8serrors.IsNotFound(err) {
			return nil
		}
		lockErr.Err = fmt.Errorf("failed to retrieve lock info: %s", err)
		return lockErr
	}

	lockErr.Info = lockInfo

	if lockInfo.ID != id {
		lockErr.Err = fmt.Errorf("lock id %q does not match existing lock", id)
		return lockErr
	}

	err = c.deleteSecret(lName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			lockErr.Err = err
			return lockErr
		}
	}
	return nil
}

func (c *RemoteClient) getLockInfo(name string) (*state.LockInfo, error) {
	secret, err := c.k8sClient.CoreV1().Secrets(c.namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	lockInfo := &state.LockInfo{}
	err = json.Unmarshal([]byte(secret.Data["info"]), lockInfo)
	if err != nil {
		return nil, err
	}

	return lockInfo, nil
}

func (c *RemoteClient) deleteSecret(name string) error {
	delProp := metav1.DeletePropagationBackground
	delOps := &metav1.DeleteOptions{PropagationPolicy: &delProp}
	return c.k8sClient.CoreV1().Secrets(c.namespace).Delete(name, delOps)
}

func compressState(data []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	gz := gzip.NewWriter(b)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func uncompressState(data []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	b.ReadFrom(gz)
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func createSecretName(s string) string {
	return terraState + s
}

// parseSecretName takes in the secret and parses it. Validates it starts with
// "terrastate-" and has an ending.
func parseSecretName(secret string) (string, bool) {
	if strings.HasPrefix(secret, terraState) {
		pieces := strings.Split(secret, terraState)
		if len(pieces) == 2 && pieces[0] == "" {
			return pieces[1], true
		}
	}
	return "", false
}

func createLockName(s string) string {
	return terraLock + s
}

func parseLockName(s string) (string, bool) {
	if strings.HasPrefix(s, terraLock) {
		pieces := strings.Split(s, terraLock)
		if len(pieces) == 2 && pieces[0] == "" {
			return pieces[1], true
		}
	}
	return "", false
}
