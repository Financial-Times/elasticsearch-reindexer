package service

import (
	"net/http"

	log "github.com/Financial-Times/go-logger"
	awsauth "github.com/smartystreets/go-aws-auth"
	"gopkg.in/olivere/elastic.v5"
)

type EsAccessConfig struct {
	esEndpoint     string
	esTraceLogging bool
	esAuthType     string
	accessKey      string
	secretKey      string
}

func NewAccessConfig(endpoint string, traceLogging bool, authType string, accessKey string, secretKey string) EsAccessConfig {
	return EsAccessConfig{accessKey: accessKey, secretKey: secretKey, esEndpoint: endpoint, esAuthType: authType, esTraceLogging: traceLogging}
}

type AWSSigningTransport struct {
	HTTPClient  *http.Client
	Credentials awsauth.Credentials
}

// RoundTrip implementation
func (a AWSSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return a.HTTPClient.Do(awsauth.Sign4(req, a.Credentials))
}

func newAmazonClient(config EsAccessConfig) (*elastic.Client, error) {
	signingTransport := AWSSigningTransport{
		Credentials: awsauth.Credentials{
			AccessKeyID:     config.accessKey,
			SecretAccessKey: config.secretKey,
		},
		HTTPClient: http.DefaultClient,
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
