package service

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/Financial-Times/go-logger"
	"github.com/aws/aws-sdk-go/aws/credentials"
	awsSigner "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/olivere/elastic/v7"
)

type EsAccessConfig struct {
	esEndpoint     string
	esTraceLogging bool
	esAuthType     string
	esRegion       string
	awsCreds       *credentials.Credentials
}

func NewAccessConfig(awsCreds *credentials.Credentials, esRegion string, endpoint string, traceLogging bool, authType string) EsAccessConfig {
	return EsAccessConfig{awsCreds: awsCreds, esRegion: esRegion, esEndpoint: endpoint, esAuthType: authType, esTraceLogging: traceLogging}
}

type AWSSigningTransport struct {
	HTTPClient  *http.Client
	credentials *credentials.Credentials
	region      string
}

// RoundTrip implementation
func (a AWSSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	signer := awsSigner.NewSigner(a.credentials)
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body with error: %w", err)
		}
		body := strings.NewReader(string(b))
		defer req.Body.Close()
		_, err = signer.Sign(req, body, "es-reindexer", a.region, time.Now())
		if err != nil {
			return nil, fmt.Errorf("failed to sign request: %w", err)
		}
	} else {
		_, err := signer.Sign(req, nil, "es-reindexer", a.region, time.Now())
		if err != nil {
			return nil, fmt.Errorf("failed to sign request: %w", err)
		}
	}

	return a.HTTPClient.Do(req)
}

func newAmazonClient(config EsAccessConfig) (*elastic.Client, error) {
	signingTransport := AWSSigningTransport{
		credentials: config.awsCreds,
		region:      config.esRegion,
		HTTPClient:  http.DefaultClient,
	}
	signingClient := &http.Client{Transport: http.RoundTripper(signingTransport)}

	log.Infof("connecting with AWSSigningTransport to %s", config.esEndpoint)
	return newClient(config.esEndpoint, config.esTraceLogging,
		elastic.SetScheme("https"),
		elastic.SetHttpClient(signingClient),
	)
}

func newSimpleClient(config EsAccessConfig) (*elastic.Client, error) {
	log.Infof("connecting with default transport to %s", config.esEndpoint)
	return newClient(config.esEndpoint, config.esTraceLogging)
}

func newClient(endpoint string, traceLogging bool, options ...elastic.ClientOptionFunc) (*elastic.Client, error) {
	optionFuncs := []elastic.ClientOptionFunc{
		elastic.SetURL(endpoint),
		elastic.SetSniff(false), //needs to be disabled due to EAS behavior. Healthcheck still operates as normal.
	}
	optionFuncs = append(optionFuncs, options...)

	if traceLogging {
		optionFuncs = append(optionFuncs, elastic.SetTraceLog(log.Logger()))
	}

	return elastic.NewClient(optionFuncs...)
}

func NewElasticClient(config EsAccessConfig) (*elastic.Client, error) {
	if config.esAuthType == "local" {
		return newSimpleClient(config)
	} else {
		return newAmazonClient(config)
	}
}
