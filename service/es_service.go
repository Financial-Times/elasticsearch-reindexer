package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

const (
	deweyUrl = "https://dewey.ft.com/TODO.html"
)

var (
	ErrNoElasticClient error = errors.New("No ElasticSearch client available")
)

type esService struct {
	sync.RWMutex
	elasticClient *elastic.Client
	indexName     string
}

type EsService interface {
	ReadData(conceptType string, uuid string) (*elastic.GetResult, error)
}

type EsHealthService interface {
	GoodToGo(writer http.ResponseWriter, req *http.Request)
	ConnectivityHealthyCheck() fthealth.Check
	ClusterIsHealthyCheck() fthealth.Check
}

func NewEsService(ch chan *elastic.Client, indexName string) *esService {
	es := &esService{indexName: indexName}
	go func() {
		for ec := range ch {
			es.setElasticClient(ec)
		}
	}()
	return es
}

func (es *esService) setElasticClient(ec *elastic.Client) {
	es.Lock()
	defer es.Unlock()

	es.elasticClient = ec
	log.Info("injected ElasticSearch connection")

	go es.MigrateIndex()
}

//GoodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (service *esService) GoodToGo(writer http.ResponseWriter, req *http.Request) {
	if _, err := service.healthChecker(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (es *esService) GetClusterHealth() (*elastic.ClusterHealthResponse, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	return es.elasticClient.ClusterHealth().Do(context.Background())
}

func (es *esService) checkElasticClient() error {
	if es.elasticClient == nil {
		return ErrNoElasticClient
	}

	return nil
}

func (service *esService) esClient() *elastic.Client {
	service.RLock()
	defer service.RUnlock()
	return service.elasticClient
}

func (es *esService) ReadData(conceptType string, uuid string) (*elastic.GetResult, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := es.elasticClient.Get().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		Do(context.Background())

	if elastic.IsNotFound(err) {
		return &elastic.GetResult{Found: false}, nil
	} else {
		return resp, err
	}
}

func (service *esService) ClusterIsHealthyCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Full or partial degradation in serving requests from Elasticsearch",
		Name:             "Check Elasticsearch cluster health",
		PanicGuide:       deweyUrl,
		Severity:         1,
		TechnicalSummary: "Elasticsearch cluster is not healthy.",
		Checker:          service.healthChecker,
	}
}

func (service *esService) healthChecker() (string, error) {
	if service.esClient() != nil {
		output, err := service.GetClusterHealth()
		if err != nil {
			return "Cluster is not healthy: ", err
		} else if output.Status != "green" {
			return fmt.Sprintf("Cluster is %v", output.Status), errors.New(fmt.Sprintf("Cluster is %v", output.Status))
		}
		return "Cluster is healthy", nil
	}

	return "Couldn't check the cluster's health.", errors.New("Couldn't establish connectivity.")
}

func (service *esService) ConnectivityHealthyCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Could not connect to Elasticsearch",
		Name:             "Check connectivity to the Elasticsearch cluster",
		PanicGuide:       deweyUrl,
		Severity:         1,
		TechnicalSummary: "Connection to Elasticsearch cluster could not be created. Please check your AWS credentials.",
		Checker:          service.connectivityChecker,
	}
}

func (service *esService) connectivityChecker() (string, error) {
	if service.esClient() == nil {
		return "", errors.New("Could not connect to elasticsearch, please check the application parameters/env variables, and restart the service.")
	}

	_, err := service.GetClusterHealth()
	if err != nil {
		return "Could not connect to elasticsearch", err
	}
	return "Successfully connected to the cluster", nil
}

func (es *esService) MigrateIndex() {
	log.Infof("Checking alias for %s", es.indexName)

	log.Infof("Creating new version of index")

	log.Infof("Making current index read-only")

	log.Infof("Reindexing")

	log.Infof("Waiting for reindexing task to complete")

	log.Infof("Cutting alias across to new index")
}
