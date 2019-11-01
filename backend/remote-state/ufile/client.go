package ufile

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	"github.com/ucloud/ucloud-sdk-go/private/services/ubusinessgroup"
	"github.com/ucloud/ucloud-sdk-go/services/ufile"
	"github.com/ucloud/ucloud-sdk-go/ucloud"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"
	"log"
	"time"
)

type RemoteClient struct {
	ufileClient *ufile.UFileClient
	ufileConfig *ufsdk.Config
	tagClient   *ubusinessgroup.UBusinessGroupClient
	bucketName  string
	stateFile   string
	lockFile    string
}

var (
	// The amount of time we will retry a state waiting for it to match the
	// expected checksum.
	consistencyRetryTimeout = 10 * time.Second

	// delay when polling the state
	consistencyRetryPollInterval = 2 * time.Second
)

const lockPrefix = "terraform-lock"

func (c *RemoteClient) Get() (payload *remote.Payload, err error) {
	deadline := time.Now().Add(consistencyRetryTimeout)

	for {
		payload, _, err = c.getObject(c.stateFile)
		if err != nil {
			log.Printf("[WARN] failed to Get UFile: %s", err)
			if time.Now().Before(deadline) {
				time.Sleep(consistencyRetryPollInterval)
				log.Println("[INFO] retrying get UFile...")
				continue
			}

			return nil, fmt.Errorf("err on geting ufile %s", err)
		}
		break
	}
	return payload, err
}

func (c *RemoteClient) Put(data []byte) error {
	if err := c.putObject(c.stateFile, data); err != nil {
		return fmt.Errorf("Failed to upload state to %v: %v", c.stateFile, err)
	}

	return nil
}

func (c *RemoteClient) Delete() error {
	if err := c.deleteObject(c.stateFile); err != nil {
		return fmt.Errorf("Failed to delete state to %v: %v", c.stateFile, err)
	}
	return nil
}

func (c *RemoteClient) Lock(info *state.LockInfo) (string, error) {
	log.Printf("[DEBUG] lock remote state file %s", c.lockFile)

	key := fmt.Sprintf("%s:%s:%s", lockPrefix, c.bucketName, c.lockFile)
	//key := fmt.Sprintf("%s:%s", lockPrefix, md5.Sum([]byte(path)))
	if err := c.CreateTag(key); err != nil {
		return "", err
	}

	tagId, err := c.DescribeTag(key)
	if err != nil {
		return "", err
	}

	defer c.DeleteTag(tagId)

	reqFile, err := ufsdk.NewFileRequest(c.ufileConfig, nil)
	if err != nil {
		return "", err
	}
	// get
	var buf []byte
	buffer := bufio.NewWriter(bytes.NewBuffer(buf))
	err = reqFile.DownloadFile(buffer, c.lockFile)
	if err != nil {
		if !(reqFile.LastResponseStatus == 404) {
			return "", fmt.Errorf("lock file %s exists, err", c.lockFile)
		}
	}

	info.Path = c.lockFile

	if info.ID == "" {
		lockID, err := uuid.GenerateUUID()
		if err != nil {
			return "", err
		}

		info.ID = lockID
	}

	//check := fmt.Sprintf("%x", md5.Sum(info.Marshal()))
	err = c.putObject(c.lockFile, info.Marshal())
	if err != nil {
		return "", err
	}

	return info.ID, nil
}

func (c *RemoteClient) Unlock(id string) error {
	info, err := c.getLockInfo()
	if err != nil {
		return err
	}

	if info.ID != id {
		return fmt.Errorf("lock id mismatch, %v != %v", info.ID, id)
	}

	err = c.deleteObject(c.lockFile)
	if err != nil {
		return err
	}

	return nil
}

func (c *RemoteClient) getLockInfo() (*state.LockInfo, error) {
	payload, exist, err := c.getObject(c.lockFile)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, fmt.Errorf("lock file %s not exists", c.lockFile)
	}

	info := &state.LockInfo{}
	if err := json.Unmarshal(payload.Data, info); err != nil {
		return nil, err
	}

	return info, nil
}

func (c *RemoteClient) putObject(file string, data []byte) error {
	reqFile, err := ufsdk.NewFileRequest(c.ufileConfig, nil)
	if err != nil {
		return err
	}
	state, err := reqFile.InitiateMultipartUpload(file, "application/json")
	if err != nil {
		return fmt.Errorf("error on upload file, %s, details: %s", err, reqFile.DumpResponse(true))
	}

	if err := reqFile.UploadPart(bytes.NewBuffer(data), state, 0); err != nil {
		reqFile.AbortMultipartUpload(state)
		return err
	}

	if err := reqFile.FinishMultipartUpload(state); err != nil {
		return err
	}

	return err
}

func (c *RemoteClient) getObject(file string) (payload *remote.Payload, exist bool, err error) {
	reqFile, err := ufsdk.NewFileRequest(c.ufileConfig, nil)
	if err != nil {
		return
	}
	var buf []byte
	buffer := bufio.NewWriter(bytes.NewBuffer(buf))
	err = reqFile.DownloadFile(buffer, file)
	if err != nil {
		if reqFile.LastResponseStatus == 404 {
			return nil, false, nil
		}
		return
	}
	exist = true
	sum := md5.Sum(reqFile.LastResponseBody)
	payload = &remote.Payload{
		Data: reqFile.LastResponseBody,
		MD5:  sum[:],
	}

	return
}

func (c *RemoteClient) deleteObject(file string) error {
	reqFile, err := ufsdk.NewFileRequest(c.ufileConfig, nil)
	if err != nil {
		return fmt.Errorf("error on building delete file request, %s", err)
	}

	if err := reqFile.DeleteFile(file); err != nil {
		return err
	}
	return nil
}

func (c *RemoteClient) CreateTag(key string) error {
	request := c.tagClient.NewCreateBusinessGroupRequest()
	request.BusinessName = ucloud.String(key)

	_, err := c.tagClient.CreateBusinessGroup(request)
	if err != nil {
		return err
	}

	return nil
}

func (c *RemoteClient) DescribeTag(key string) (string, error) {
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

	return "", fmt.Errorf("not found error")
}

func (c *RemoteClient) DeleteTag(tagId string) error {
	request := c.tagClient.NewDeleteBusinessGroupRequest()
	request.BusinessId = ucloud.String(tagId)

	_, err := c.tagClient.DeleteBusinessGroup(request)

	if err != nil {
		return err
	}

	return nil
}
