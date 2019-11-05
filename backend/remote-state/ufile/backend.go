package ufile

import (
	"context"
	"errors"
	"fmt"
	"github.com/ucloud/ucloud-sdk-go/private/services/ubusinessgroup"
	"github.com/ucloud/ucloud-sdk-go/ucloud/auth"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform/backend"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/version"
	"github.com/ucloud/ucloud-sdk-go/services/ufile"
	"github.com/ucloud/ucloud-sdk-go/ucloud"
	ufsdk "github.com/ufilesdk-dev/ufile-gosdk"
)

// New creates a new backend for ufile remote state.
func New() backend.Backend {
	s := &schema.Backend{
		Schema: map[string]*schema.Schema{
			"public_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("UCLOUD_PUBLIC_KEY", nil),
				Description: "UCloud Public Key ID",
			},

			"private_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("UCLOUD_PRIVATE_KEY", nil),
				Description: "UCloud Private Key ID",
			},

			"project_id": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("UCLOUD_PROJECT_ID", nil),
				Description: "UCloud Project ID",
			},

			"region": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("UCLOUD_REGION", nil),
				Description: "The region of the UFlile bucket",
			},

			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the UFile bucket",
			},

			"key": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The path to the state file inside the bucket",
				ValidateFunc: func(v interface{}, s string) ([]string, []error) {
					// ufile will strip leading slashes from an object, so while this will
					// technically be accepted by ufile, it will break our workspace hierarchy.
					if strings.HasPrefix(v.(string), "/") {
						return nil, []error{errors.New("key must not start with '/'")}
					}
					return nil, nil
				},
			},

			"prefix": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The directory where state files will be saved inside the bucket",
				Default:     "env:",
				ValidateFunc: func(v interface{}, s string) ([]string, []error) {
					prefix := v.(string)
					if strings.HasPrefix(prefix, "/") || strings.HasSuffix(prefix, "/") {
						return nil, []error{fmt.Errorf("prefix must not start or end with '/'")}
					}
					return nil, nil
				},
			},

			"base_url": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "UCloud Base URL",
				ValidateFunc: func(v interface{}, k string) ([]string, []error) {
					baseUrl := v.(string)

					if _, err := url.Parse(baseUrl); err != nil {
						return nil, []error{fmt.Errorf("%q is invalid, should be an valid ucloud base_url, got %q, parse error: %s", "base_url", baseUrl, err)}
					}
					return nil, nil
				},
			},
		},
	}

	result := &Backend{Backend: s}
	result.Backend.ConfigureFunc = result.configure
	return result
}

type Backend struct {
	*schema.Backend

	// The fields below are set from configure
	ufileClient *ufsdk.UFileRequest
	tagClient   *ubusinessgroup.UBusinessGroupClient

	bucketName string
	keyName    string
	prefix     string
}

func (b *Backend) configure(ctx context.Context) error {
	d := schema.FromContextBackendConfig(ctx)

	b.bucketName = d.Get("bucket").(string)
	b.keyName = d.Get("key").(string)
	b.prefix = d.Get("prefix").(string)

	cfg := ucloud.NewConfig()
	cfg.Region = d.Get("region").(string)
	cfg.ProjectId = d.Get("project_id").(string)
	cfg.UserAgent = fmt.Sprintf("Backend-UCloud/%s", version.Version)

	// set default max retry count
	cfg.MaxRetries = 3
	if v, ok := d.GetOk("base_url"); ok {
		cfg.BaseUrl = v.(string)
	}

	cred := auth.NewCredential()
	cred.PublicKey = d.Get("public_key").(string)
	cred.PrivateKey = d.Get("private_key").(string)

	ufileClient := ufile.NewClient(&cfg, &cred)
	b.tagClient = ubusinessgroup.NewClient(&cfg, &cred)

	// set the ufile config
	var bucketHost string
	if v, ok := d.GetOk("base_url"); ok {
		// skip error because it has been validated by prepare
		urlObj, _ := url.Parse(v.(string))
		bucketHost = urlObj.Host
	} else {
		bucketHost = "api.ucloud.cn"
	}

	domain, err := queryBucket(ufileClient, d.Get("bucket").(string))
	if err != nil {
		return fmt.Errorf("Failed to query bucket, %s", err)
	}

	fileHost := strings.SplitN(domain, ".", 2)[1]
	config := &ufsdk.Config{
		PublicKey:  d.Get("public_key").(string),
		PrivateKey: d.Get("private_key").(string),
		BucketName: d.Get("bucket").(string),
		FileHost:   fileHost,
		BucketHost: bucketHost,
	}

	reqFile, err := ufsdk.NewFileRequest(config, nil)
	if err != nil {
		return fmt.Errorf("Failed to build UFile request, %s", err)
	}

	b.ufileClient = reqFile

	return nil
}

func queryBucket(conn *ufile.UFileClient, bucketName string) (string, error) {
	req := conn.NewDescribeBucketRequest()
	req.BucketName = ucloud.String(bucketName)
	resp, err := conn.DescribeBucket(req)
	if err != nil {
		return "", fmt.Errorf("error on reading bucket %q when create bucket, %s", bucketName, err)
	}

	if len(resp.DataSet) < 1 {
		return "", fmt.Errorf("the bucket %s is not exit", bucketName)
	}

	return resp.DataSet[0].Domain.Src[0], nil
}
