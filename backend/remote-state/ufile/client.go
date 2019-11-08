package ufile

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	"github.com/ucloud/ucloud-sdk-go/private/services/ubusinessgroup"
	"github.com/ucloud/ucloud-sdk-go/ucloud"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"
)

type remoteClient struct {
	ufileClient *ufsdk.UFileRequest
	tagClient   *ubusinessgroup.UBusinessGroupClient
	bucketName  string
	stateFile   string
	lockFile    string
}

const lockPrefix = "terraform-lock"

func (c *remoteClient) Get() (payload *remote.Payload, err error) {
	payload, exist, err := c.getObject(c.stateFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to geting state file at %v: %s", c.stateFileURL(), err)
	}

	if !exist {
		return nil, nil
	}

	return payload, nil
}

func (c *remoteClient) Put(data []byte) error {
	if err := c.putObject(c.stateFile, data); err != nil {
		return fmt.Errorf("Failed to upload state file to %v: %s", c.stateFileURL(), err)
	}

	return nil
}

func (c *remoteClient) Delete() error {
	if err := c.deleteObject(c.stateFile); err != nil {
		return fmt.Errorf("Failed to delete state file to %v: %s", c.stateFileURL(), err)
	}
	return nil
}

func (c *remoteClient) delete() error {
	if err := c.deleteObject(c.stateFile); err != nil {
		return fmt.Errorf("Failed to delete state file to %v: %s", c.stateFileURL(), err)
	}
	return nil
}

func (c *remoteClient) Lock(info *state.LockInfo) (string, error) {
	key := fmt.Sprintf("%s:%s:%s", lockPrefix, c.bucketName, c.lockFile)

	tagId, err := c.ufileLock(key)
	if err != nil {
		return "", c.lockError(err)
	}

	_, exist, err := c.getObject(c.lockFile)
	if err != nil {
		err = fmt.Errorf("Failed to geting lock file at %v: %s", c.lockFileURL(), err)
	}
	if exist {
		err = fmt.Errorf("Lock file exist at %v", c.lockFileURL())
	}
	if err != nil {
		return "", c.lockError(c.ufileUnlock(tagId, err))
	}

	info.Path = c.lockFileURL()

	if info.ID == "" {
		lockID, err := uuid.GenerateUUID()
		if err != nil {
			return "", c.lockError(c.ufileUnlock(tagId, err))
		}

		info.ID = lockID
	}

	if c.putObject(c.lockFile, info.Marshal()) != nil {
		err = fmt.Errorf("Failed to put lock file at %v: %s", c.lockFileURL(), err)
		return "", c.lockError(c.ufileUnlock(tagId, err))
	}

	if err = c.ufileUnlock(tagId, nil); err != nil {
		return "", c.lockError(err)
	}

	return info.ID, nil
}

func (c *remoteClient) Unlock(id string) error {
	info, err := c.lockInfo()
	if err != nil {
		return c.lockError(err)
	}

	if info.ID != id {
		return c.lockError(fmt.Errorf("lock ID %q does not match existing lock %q", id, info.ID))
	}

	err = c.deleteObject(c.lockFile)
	if err != nil {
		return c.lockError(err)
	}

	key := fmt.Sprintf("%s:%s:%s", lockPrefix, c.bucketName, c.lockFile)
	tagId, err := c.DescribeTag(key)
	if err != nil {
		if isNotExistError(err) {
			return nil
		}
		return c.lockError(err)
	}

	return c.DeleteTag(tagId)
}

func (c *remoteClient) lockError(err error) *state.LockError {
	lockErr := &state.LockError{
		Err: err,
	}

	info, infoErr := c.lockInfo()
	if infoErr != nil {
		lockErr.Err = multierror.Append(lockErr.Err, infoErr)
	} else {
		lockErr.Info = info
	}
	return lockErr
}

func (c *remoteClient) lockInfo() (*state.LockInfo, error) {
	payload, exist, err := c.getObject(c.lockFile)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, newNotExistError(fmt.Sprintf("lock file %s", c.lockFile))
	}

	info := &state.LockInfo{}
	if err := json.Unmarshal(payload.Data, info); err != nil {
		return nil, err
	}

	return info, nil
}

func (c *remoteClient) putObject(file string, data []byte) error {
	state, err := c.ufileClient.InitiateMultipartUpload(file, "application/json")
	if err != nil {
		return fmt.Errorf("error on initing upload file, %s", err)
	}

	if err := c.ufileClient.UploadPart(bytes.NewBuffer(data), state, 0); err != nil {
		// ignore err
		_ = c.ufileClient.AbortMultipartUpload(state)
		return fmt.Errorf("error on uploading file, %s", err)
	}

	if err := c.ufileClient.FinishMultipartUpload(state); err != nil {
		return fmt.Errorf("error on finishing upload file, %s", err)
	}

	return nil
}

func (c *remoteClient) getObject(file string) (payload *remote.Payload, exist bool, err error) {
	var buf []byte
	buffer := bufio.NewWriter(bytes.NewBuffer(buf))
	err = c.ufileClient.DownloadFile(buffer, file)
	if err != nil {
		if c.ufileClient.LastResponseStatus == 404 {
			return nil, false, nil
		}
		return
	}
	exist = true
	sum := md5.Sum(c.ufileClient.LastResponseBody)
	payload = &remote.Payload{
		Data: c.ufileClient.LastResponseBody,
		MD5:  sum[:],
	}

	return
}

func (c *remoteClient) deleteObject(file string) error {
	if err := c.ufileClient.DeleteFile(file); err != nil {
		return fmt.Errorf("error on deleting file, %s", err)
	}
	return nil
}

func (c *remoteClient) ufileLock(key string) (string, error) {
	if err := c.CreateTag(key); err != nil {
		return "", err
	}

	tagId, err := c.DescribeTag(key)
	if err != nil {
		return "", err
	}

	return tagId, nil
}

func (c *remoteClient) ufileUnlock(tagId string, err error) error {
	errTag := c.DeleteTag(tagId)
	if err != nil {
		if errTag != nil {
			return c.lockError(fmt.Errorf("%v, delete tag err: %s", err, errTag))
		}
		return c.lockError(err)
	}

	if errTag != nil {
		return errTag
	}

	return nil
}

func (c *remoteClient) CreateTag(key string) error {
	request := c.tagClient.NewCreateBusinessGroupRequest()
	request.BusinessName = ucloud.String(key)

	_, err := c.tagClient.CreateBusinessGroup(request)
	if err != nil {
		return fmt.Errorf("err on creating tag, %s", err)
	}

	return nil
}

func (c *remoteClient) DescribeTag(key string) (string, error) {
	req := c.tagClient.NewListBusinessGroupRequest()

	var allInstances []ubusinessgroup.BusinessGroupInfo
	var limit = 100
	var offset int
	for {
		req.Limit = ucloud.Int(limit)
		req.Offset = ucloud.Int(offset)
		resp, err := c.tagClient.ListBusinessGroup(req)
		if err != nil {
			return "", fmt.Errorf("error on reading tag list, %s", err)
		}

		if resp == nil || len(resp.Infos) < 1 {
			break
		}

		allInstances = append(allInstances, resp.Infos...)

		if len(resp.Infos) < limit {
			break
		}

		offset = offset + limit
	}

	for _, v := range allInstances {
		if v.BusinessName == key {
			return v.BusinessId, nil
		}
	}

	return "", newNotExistError("tag")
}

func (c *remoteClient) DeleteTag(tagId string) error {
	request := c.tagClient.NewDeleteBusinessGroupRequest()
	request.BusinessId = ucloud.String(tagId)

	_, err := c.tagClient.DeleteBusinessGroup(request)

	if err != nil {
		return fmt.Errorf("err on deleting tag, %s", err)
	}

	return nil
}

func (c *remoteClient) stateFileURL() string {
	return fmt.Sprintf("ufile://%v/%v", c.bucketName, c.stateFile)
}

func (c *remoteClient) lockFileURL() string {
	return fmt.Sprintf("ufile://%v/%v", c.bucketName, c.lockFile)
}

type notExistError struct {
	message string
}

func (e *notExistError) Error() string {
	return e.message
}

func newNotExistError(v string) error {
	return &notExistError{fmt.Sprintf("the %s is not exist", v)}
}

func isNotExistError(err error) bool {
	if _, ok := err.(*notExistError); ok {
		return true
	}
	return false
}
