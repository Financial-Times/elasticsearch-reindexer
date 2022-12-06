package service

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	log "github.com/Financial-Times/go-logger"
	"github.com/aws/aws-sdk-go/aws/credentials"
	awsSigner "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/olivere/elastic/v7"
)

type EsAccessConfig struct {
	endpoint     string
	region       string
	authType     string
	traceLogging bool
	awsCreds     *credentials.Credentials
}

func NewAccessConfig(awsCreds *credentials.Credentials, region, endpoint, authType string, traceLogging bool) EsAccessConfig {
	return EsAccessConfig{
		awsCreds:     awsCreds,
		endpoint:     endpoint,
		region:       region,
		authType:     authType,
		traceLogging: traceLogging,
	}
}

type awsSigningTransport struct {
	httpClient  *http.Client
	credentials *credentials.Credentials
	region      string
}

func (t awsSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	signer := awsSigner.NewSigner(t.credentials)

	var body io.ReadSeeker
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("reading request body: %w", err)
		}
		body = bytes.NewReader(b)
		defer req.Body.Close()
	}

	_, err := signer.Sign(req, body, "es", t.region, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("signing request: %w", err)
	}

	return t.httpClient.Do(req)
}

func newAmazonClient(config EsAccessConfig) (*elastic.Client, error) {
	signingTransport := awsSigningTransport{
		credentials: config.awsCreds,
		region:      config.region,
		httpClient:  http.DefaultClient,
	}
	signingClient := &http.Client{
		Transport: signingTransport,
	}

	return newClient(config.endpoint, config.traceLogging,
		elastic.SetScheme("https"),
		elastic.SetHttpClient(signingClient),
	)
}

func newSimpleClient(config EsAccessConfig) (*elastic.Client, error) {
	return newClient(config.endpoint, config.traceLogging)
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
	if config.authType == "local" {
		return newSimpleClient(config)
	} else {
		return newAmazonClient(config)
	}
}
