package kubernetes

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes"
)

const (
	terraState     = "terrastate"
	terraKey       = "terraKey"
	terraWorkspace = "terraWorkspace"
)

type RemoteClient struct {
	k8sClient  kubernetes.Interface
	namespace  string
	nameSuffix string
	workspace  string
}

func (c *RemoteClient) Get() (payload *remote.Payload, err error) {
	sName := c.createSecretName()
	secret, err := c.k8sClient.CoreV1().Secrets(c.namespace).Get(sName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	stateRaw, ok := secret.Data[terraState]
	if !ok {
		// The secret exists but there is no state in it
		return nil, nil
	}

	state, err := uncompressState(stateRaw)
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
	sName := c.createSecretName()

	payload, err := compressState(data)
	if err != nil {
		return err
	}

	secret, err := c.getSecret(sName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return err
		}
		return fmt.Errorf("secret does not exist so lock is not held, secret name: %v err: %v", sName, err)
	}

	lockInfo, err := c.getLockInfo(secret)
	if err != nil {
		return err
	}

	secret.Data[terraState] = payload

	secret, err = c.k8sClient.CoreV1().Secrets(c.namespace).Update(secret)
	if err != nil {
		lockErr := &state.LockError{
			Info: lockInfo,
			Err:  fmt.Errorf("error updating the state: %v", err),
		}
		return lockErr
	}

	return err
}

// Delete the state secret
func (c *RemoteClient) Delete() error {
	sName := c.createSecretName()

	err := c.deleteSecret(sName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (c *RemoteClient) Lock(info *state.LockInfo) (string, error) {
	sName := c.createSecretName()

	lockInfo := info.Marshal()

	secret, err := c.getSecret(sName)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return "", err
		}

		// The secret doesn't exist yet, create it with the lock
		sData := make(map[string][]byte)
		sData["lockInfo"] = lockInfo

		label := map[string]string{
			terraState:     "true",
			terraKey:       c.nameSuffix,
			terraWorkspace: c.workspace,
		}
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sName,
				Namespace: c.namespace,
				Labels:    label,
			},
			Data: sData,
		}

		_, err = c.k8sClient.CoreV1().Secrets(c.namespace).Create(secret)
		if err != nil {
			if !k8serrors.IsAlreadyExists(err) {
				return "", err
			}
			// The secret was created between the get and create, grab it and keep going
			secret, err = c.getSecret(sName)
			if err != nil {
				return "", err
			}
		} else {
			// No error on the create so the lock has been created successfully
			return info.ID, nil
		}
	}

	li, err := c.getLockInfo(secret)
	if err != nil {
		return "", err
	}

	if li != nil {
		// The lock already exists
		lockErr := &state.LockError{
			Info: li,
			Err:  errors.New("lock already exists"),
		}
		return "", lockErr
	}

	secret.Data["lockInfo"] = lockInfo
	_, err = c.k8sClient.CoreV1().Secrets(c.namespace).Update(secret)
	if err != nil {
		return "", err
	}

	return info.ID, err
}

func (c *RemoteClient) Unlock(id string) error {
	sName := c.createSecretName()

	secret, err := c.getSecret(sName)
	if err != nil {
		// If the secret doesn't exist, there is nothing to unlock
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	lockInfo, err := c.getLockInfo(secret)
	if err != nil {
		return err
	}

	if lockInfo == nil {
		// The lock doesn't exist
		return nil
	}

	lockErr := &state.LockError{
		Info: lockInfo,
	}

	if lockInfo.ID != id {
		lockErr.Err = fmt.Errorf("lock id %q does not match existing lock", id)
		return lockErr
	}

	secret.Data["lockInfo"] = []byte{}

	_, err = c.k8sClient.CoreV1().Secrets(c.namespace).Update(secret)
	if err != nil {
		lockErr.Err = err
		return lockErr
	}

	return nil
}

//getLockInfo takes a secret and attemps to read the lockInfo field.
func (c *RemoteClient) getLockInfo(secret *corev1.Secret) (*state.LockInfo, error) {
	lockData, ok := secret.Data["lockInfo"]
	if len(lockData) == 0 || !ok {
		return nil, nil
	}

	lockInfo := &state.LockInfo{}
	err := json.Unmarshal([]byte(lockData), lockInfo)
	if err != nil {
		return nil, err
	}

	return lockInfo, nil
}

func (c *RemoteClient) getSecret(name string) (*corev1.Secret, error) {
	return c.k8sClient.CoreV1().Secrets(c.namespace).Get(name, metav1.GetOptions{})
}

func (c *RemoteClient) deleteSecret(name string) error {
	delProp := metav1.DeletePropagationBackground
	delOps := &metav1.DeleteOptions{PropagationPolicy: &delProp}
	return c.k8sClient.CoreV1().Secrets(c.namespace).Delete(name, delOps)
}

func (c *RemoteClient) createSecretName() string {
	s := strings.Join([]string{c.workspace, c.nameSuffix}, "/")
	hasher := sha256.New()
	hasher.Write([]byte(s))
	suffix := base32.StdEncoding.WithPadding(-1).EncodeToString(hasher.Sum(nil))[:10]

	return terraState + "-" + strings.ToLower(suffix)
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
