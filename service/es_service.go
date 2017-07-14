package service

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

var (
	ErrNoIndexVersion  error = errors.New("No index version has been specified")
	ErrNoElasticClient error = errors.New("No ElasticSearch client available")
	ErrInvalidAlias    error = errors.New("ElasticSearch alias configuration is invalid for update")
)

type EsService interface {
	MigrateIndex(aliasName string, mappingFile string) error
}

type EsHealthService interface {
	GoodToGo(writer http.ResponseWriter, req *http.Request)
	ConnectivityHealthyCheck() fthealth.Check
	ClusterIsHealthyCheck() fthealth.Check
	IndexMappingsCheck() fthealth.Check
}

type esService struct {
	sync.RWMutex
	elasticClient       *elastic.Client
	aliasName           string
	mappingFile         string
	indexVersion        string
	pollReindexInterval time.Duration
	migrationCheck      bool
	migrationErr        error
	panicGuideUrl       string
}

func NewEsService(ch chan *elastic.Client, aliasName string, mappingFile string, indexVersion string, panicGuideUrl string) *esService {
	es := &esService{aliasName: aliasName, mappingFile: mappingFile, indexVersion: indexVersion, pollReindexInterval: time.Minute, panicGuideUrl: panicGuideUrl}
	go func() {
		for ec := range ch {
			es.setElasticClient(ec)
			es.migrationErr = es.MigrateIndex(es.aliasName, es.mappingFile)
			es.migrationCheck = true
		}
	}()
	return es
}

func (es *esService) setElasticClient(ec *elastic.Client) {
	es.Lock()
	defer es.Unlock()

	es.elasticClient = ec
	log.Info("injected ElasticSearch connection")
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

func (service *esService) ClusterIsHealthyCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Full or partial degradation in serving requests from Elasticsearch",
		Name:             "Check Elasticsearch cluster health",
		PanicGuide:       service.panicGuideUrl,
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
		PanicGuide:       service.panicGuideUrl,
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

func (service *esService) IndexMappingsCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Search results may not be as expected for the data set.",
		Name:             "Check Elasticsearch mappings version",
		PanicGuide:       service.panicGuideUrl,
		Severity:         2,
		TechnicalSummary: "Elasticsearch mappings may not have been migrated.",
		Checker:          service.mappingsChecker,
	}
}

func (service *esService) mappingsChecker() (string, error) {
	if service.migrationErr != nil {
		return "Elasticsearch mappings were not migrated successfully", service.migrationErr
	}

	if !service.migrationCheck {
		return "Elasticsearch mappings migration is in progress", fmt.Errorf("Elasticsearch mappings migration to version %s is in progress", service.indexVersion)
	}

	return fmt.Sprintf("Elasticsearch mappings are at version %s", service.indexVersion), nil
}

func (es *esService) MigrateIndex(aliasName string, mappingFile string) error {
	if len(es.indexVersion) == 0 {
		log.Error(ErrNoIndexVersion.Error())
		return ErrNoIndexVersion
	}

	if _, err := es.healthChecker(); err != nil {
		log.WithError(err).Error("cluster is not healthy")
		return err
	}

	client := es.esClient()

	requireUpdate, currentIndexName, newIndexName, err := es.checkIndexAliases(client, aliasName)
	if err != nil {
		log.WithError(err).Error("unable to read alias definition")
		return err
	}
	if !requireUpdate {
		log.WithField("index", es.indexVersion).Info("index is up-to-date")
		return nil
	}

	mapping, err := ioutil.ReadFile(mappingFile)
	if err != nil {
		log.WithError(err).Error("unable to read new index mapping definition")
		return err
	}

	err = es.createIndex(client, newIndexName, string(mapping))
	if err != nil {
		log.WithError(err).Error("unable to create new index")
		return err
	}

	if len(currentIndexName) > 0 {
		err = es.setReadOnly(client, currentIndexName)
		if err != nil {
			log.WithError(err).Error("unable to set index read-only")
			return err
		}

		completeCount, err := es.reindex(client, currentIndexName, newIndexName)
		if err != nil {
			log.WithError(err).Error("failed to begin reindex")
			return err
		}

		taskErrCount := 0
		for {
			finished, err := es.isTaskComplete(client, newIndexName, completeCount)
			if err != nil {
				log.WithError(err).Error("failed to obtain reindex task status")
				taskErrCount++
				if taskErrCount == 3 {
					return err
				}
			}

			if finished {
				break
			}

			time.Sleep(es.pollReindexInterval)
		}
	}

	err = es.updateAlias(client, aliasName, currentIndexName, newIndexName)
	if err != nil {
		log.WithError(err).Error("failed to update alias")
		return err
	}

	log.WithFields(log.Fields{"from": currentIndexName, "to": newIndexName}).Info("index migration completed")
	return nil
}

func (es *esService) checkIndexAliases(client *elastic.Client, aliasName string) (bool, string, string, error) {
	aliasesService := elastic.NewAliasesService(client)
	aliasesResult, err := aliasesService.Do(context.Background())
	if err != nil {
		return false, "", "", err
	}

	aliasedIndices := aliasesResult.IndicesByAlias(aliasName)
	switch len(aliasedIndices) {
	case 0:
		log.WithField("alias", aliasName).Info("no current index alias")
		requiredIndex := fmt.Sprintf("%s-%s", aliasName, es.indexVersion)

		return true, "", requiredIndex, nil

	case 1:
		log.WithFields(log.Fields{"alias": aliasName, "index": aliasedIndices[0]}).Info("current index alias")
		requiredIndex := fmt.Sprintf("%s-%s", aliasName, es.indexVersion)
		log.WithField("index", requiredIndex).Info("comparing to required index alias")

		return !(aliasedIndices[0] == requiredIndex), aliasedIndices[0], requiredIndex, nil

	default:
		log.WithFields(log.Fields{"alias": aliasName, "indices": aliasedIndices}).Error("alias points to multiple indices")
		return false, "", "", ErrInvalidAlias
	}
}

func (es *esService) createIndex(client *elastic.Client, indexName string, indexMapping string) error {
	log.WithFields(log.Fields{"indexName": indexName, "mapping": indexMapping}).Info("Creating new index")

	indexService := elastic.NewIndicesCreateService(client)
	_, err := indexService.Index(indexName).BodyString(indexMapping).Do(context.Background())

	return err
}

func (es *esService) setReadOnly(client *elastic.Client, indexName string) error {
	log.WithField("index", indexName).Info("Setting to read-only")

	indexService := elastic.NewIndicesPutSettingsService(client)
	_, err := indexService.Index(indexName).BodyJson(map[string]interface{}{"index.blocks.write": "true"}).Do(context.Background())

	return err
}

func (es *esService) reindex(client *elastic.Client, fromIndex string, toIndex string) (int, error) {
	log.WithFields(log.Fields{"from": fromIndex, "to": toIndex}).Info("reindexing")

	counter := elastic.NewCountService(client)
	count, err := counter.Index(toIndex).Do(context.Background())
	if err != nil {
		return 0, err
	}

	count, err = counter.Index(fromIndex).Do(context.Background())
	if err != nil {
		return 0, err
	}

	indexService := elastic.NewReindexService(client)
	_, err = indexService.SourceIndex(fromIndex).DestinationIndex(toIndex).WaitForCompletion(false).Do(context.Background())

	if err != nil {
		return 0, err
	}

	return int(count), err
}

func (es *esService) isTaskComplete(client *elastic.Client, indexName string, completeCount int) (bool, error) {
	counter := elastic.NewCountService(client)
	count, err := counter.Index(indexName).Do(context.Background())
	return int(count) == completeCount, err
}

func (es *esService) updateAlias(client *elastic.Client, aliasName string, oldIndexName string, newIndexName string) error {
	log.WithFields(log.Fields{"alias": aliasName, "from": oldIndexName, "to": newIndexName}).Info("updating index alias")

	aliasService := elastic.NewAliasService(client)
	if len(oldIndexName) > 0 {
		aliasService = aliasService.Remove(oldIndexName, aliasName)
	}

	_, err := aliasService.Add(newIndexName, aliasName).Do(context.Background())

	return err
}
