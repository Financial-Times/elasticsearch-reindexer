//go:build integration
// +build integration

package service

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/Financial-Times/go-logger"
	"github.com/Masterminds/semver"
	"github.com/google/uuid"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	apiBaseURL          = "http://test.api.ft.com"
	testIndexName       = "test-index"
	testIndexVersion    = "0.0.1"
	esTopicType         = "topics"
	ftTopicType         = "http://www.ft.com/ontology/Topic"
	testOldMappingFile  = "test/old-mapping.json"
	testNewMappingFile  = "test/new-mapping.json"
	testAliasFilterFile = "test/alias-filter.json"
	size                = 100
	aliasForAllConcepts = "aliasForAllConcepts"
)

var (
	testOldIndexName string
	testNewIndexName string
)

func getElasticSearchTestURL(t *testing.T) string {
	esURL := os.Getenv("ELASTICSEARCH_TEST_URL")
	if strings.TrimSpace(esURL) == "" {
		esURL = "http://localhost:9200"
	}

	return esURL
}

func createIndex(ec *elastic.Client, indexName string, mappingFile string) error {
	mapping, err := ioutil.ReadFile(mappingFile)
	if err != nil {
		return err
	}

	log.WithField("index", indexName).Info("test case is creating index")
	_, err = ec.CreateIndex(indexName).Body(string(mapping)).Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func createAlias(ec *elastic.Client, aliasName string, indexName string) error {
	log.WithFields(map[string]interface{}{"index": indexName, "alias": aliasName}).Info("test case is creating alias")
	_, err := ec.Alias().Add(indexName, aliasName).Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func writeTestConcepts(ec *elastic.Client, indexName string, esConceptType string, ftConceptType string, amount int) error {
	for i := 0; i < amount; i++ {
		testUUID := uuid.NewString()

		aliases := []string{}
		if i%2 == 0 {
			aliases = append(aliases, fmt.Sprintf("Test concept %s %v", esConceptType, i))
		}

		payload := map[string]interface{}{
			"id":         testUUID,
			"type":       esConceptType,
			"apiUrl":     fmt.Sprintf("%s/%s/%s", apiBaseURL, esConceptType, testUUID),
			"prefLabel":  fmt.Sprintf("Test concept %s %s", esConceptType, testUUID),
			"types":      []string{ftConceptType},
			"directType": ftConceptType,
			"aliases":    aliases,
		}

		_, err := ec.Index().
			Index(indexName).
			Id(testUUID).
			BodyJson(payload).
			Do(context.Background())
		if err != nil {
			return err
		}
	}

	// ensure test data is immediately available from the index
	_, err := ec.Refresh(indexName).Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

type EsServiceTestSuite struct {
	suite.Suite
	esURL     string
	ec        *elastic.Client
	indexName string
	service   esService
}

func TestEsServiceSuite(t *testing.T) {
	suite.Run(t, new(EsServiceTestSuite))
}

func (s *EsServiceTestSuite) SetupSuite() {
	log.InitLogger("test", "warning")
	oldVersion := semver.MustParse(testIndexVersion)
	testOldIndexName = fmt.Sprintf("%s-%s", testIndexName, oldVersion.String())
	requiredVersion := oldVersion.IncPatch()
	testNewIndexName = fmt.Sprintf("%s-%s", testIndexName, requiredVersion.String())

	s.esURL = getElasticSearchTestURL(s.T())

	ec, err := elastic.NewClient(
		elastic.SetURL(s.esURL),
		elastic.SetSniff(false),
	)
	require.NoError(s.T(), err, "expected no error for ES client")

	s.ec = ec
}

func (s *EsServiceTestSuite) TearDownSuite() {
	_, err := s.ec.IndexPutSettings().BodyJson(map[string]interface{}{"index.number_of_replicas": 0}).Do(context.Background())
	require.NoError(s.T(), err, "expected no error in modifying replica settings")
}

func (s *EsServiceTestSuite) SetupTest() {
	s.indexName = testOldIndexName

	//ignore errors
	_, _ = s.ec.Alias().Remove(testOldIndexName, testIndexName).Do(context.Background())
	_, _ = s.ec.Alias().Remove(testNewIndexName, testIndexName).Do(context.Background())
	_, _ = s.ec.Alias().Remove(testNewIndexName, aliasForAllConcepts).Do(context.Background())
	_, _ = s.ec.Alias().Remove(testNewIndexName, aliasForAllConcepts).Do(context.Background())
	_, _ = s.ec.DeleteIndex(testOldIndexName).Do(context.Background())
	_, _ = s.ec.DeleteIndex(testNewIndexName).Do(context.Background())

	err := createIndex(s.ec, testOldIndexName, testOldMappingFile)
	require.NoError(s.T(), err, "expected no error in creating index")

	err = writeTestConcepts(s.ec, testOldIndexName, esTopicType, ftTopicType, size)
	require.NoError(s.T(), err, "expected no error in adding topics")
}

func (s *EsServiceTestSuite) forCurrentIndexVersion() {
	s.service.indexVersion = testIndexVersion
}

func (s *EsServiceTestSuite) forNextIndexVersion() {
	oldVersion := semver.MustParse(testIndexVersion)
	requiredVersion := oldVersion.IncPatch()
	s.service.indexVersion = requiredVersion.String()
}

func (s *EsServiceTestSuite) TestCheckIndexAliasesMatch() {
	s.service = esService{}
	s.forCurrentIndexVersion()

	err := createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	requireUpdate, current, required, err := s.service.checkIndexAliases(s.ec, testIndexName)

	assert.NoError(s.T(), err, "expected no error for checking index")
	assert.False(s.T(), requireUpdate, "expected no update required")
	assert.Equal(s.T(), testOldIndexName, current, "current index")
	assert.Equal(s.T(), testOldIndexName, required, "required index")
}

func (s *EsServiceTestSuite) TestCheckIndexAliasesDoNotMatch() {
	s.service = esService{}
	s.forNextIndexVersion()

	err := createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	requireUpdate, current, required, err := s.service.checkIndexAliases(s.ec, testIndexName)

	assert.NoError(s.T(), err, "expected no error for checking index")
	assert.True(s.T(), requireUpdate, "expected update required")
	assert.Equal(s.T(), testOldIndexName, current, "current index")
	assert.Equal(s.T(), testNewIndexName, required, "required index")
}

func (s *EsServiceTestSuite) TestCheckIndexAliasesNotFound() {
	s.service = esService{}
	s.forCurrentIndexVersion()

	requireUpdate, currentIndexName, newIndexName, err := s.service.checkIndexAliases(s.ec, testIndexName)

	assert.NoError(s.T(), err, "expected no error for checking index")
	assert.True(s.T(), requireUpdate, "expected no update required")
	assert.Len(s.T(), currentIndexName, 0, "current index name should be empty")
	assert.Equal(s.T(), testOldIndexName, newIndexName, "required index")
}

func (s *EsServiceTestSuite) TestCheckIndexAliasesMultiple() {
	s.service = esService{}
	s.forCurrentIndexVersion()

	err := createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	err = createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error in creating index")

	err = createAlias(s.ec, testIndexName, testNewIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	requireUpdate, currentIndexName, newIndexName, err := s.service.checkIndexAliases(s.ec, testIndexName)

	assert.Error(s.T(), err, "expected an error for checking index")
	assert.Contains(s.T(), err.Error(), fmt.Sprintf("alias %s points to multiple indices", testIndexName), "error message")
	assert.Contains(s.T(), err.Error(), testOldIndexName, "error message")
	assert.Contains(s.T(), err.Error(), testNewIndexName, "error message")

	assert.False(s.T(), requireUpdate, "expected no update required")
	assert.Empty(s.T(), currentIndexName, "current index name should be empty")
	assert.Empty(s.T(), newIndexName, "required index name should be empty")
}

func (s *EsServiceTestSuite) TestCreateIndex() {
	s.service = esService{}
	s.forNextIndexVersion()
	indexMapping, err := ioutil.ReadFile(testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for reading index mapping file")

	err = s.service.createIndex(s.ec, testNewIndexName, string(indexMapping))

	assert.NoError(s.T(), err, "expected no error for creating index")

	mapping, err := s.ec.GetFieldMapping().Index(testNewIndexName).Field("prefLabel").Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for reading index mapping")
	assert.True(s.T(), hasMentionsCompletionMapping(mapping), "new index should have mentionsCompletion in its mappings")
}

func (s *EsServiceTestSuite) TestCreateIndexFailure() {
	s.service = esService{}
	s.forNextIndexVersion()
	indexMapping, err := ioutil.ReadFile(testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for reading index mapping file")

	err = s.service.createIndex(s.ec, testOldIndexName, string(indexMapping))
	assert.Error(s.T(), err, "expected error for creating index")
	assert.Regexp(s.T(), fmt.Sprintf("index.+%s.+already exists", regexp.QuoteMeta(testOldIndexName)), err.Error(), "error message")

	oldMapping, err := s.ec.GetFieldMapping().Index(testOldIndexName).Field("prefLabel").Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for reading index mapping")
	assert.False(s.T(), hasMentionsCompletionMapping(oldMapping), "old index should not have mentionsCompletion in its mappings")
}

func (s *EsServiceTestSuite) TestSetReadOnly() {
	s.service = esService{}
	s.forCurrentIndexVersion()

	err := s.service.setReadOnly(s.ec, testOldIndexName)
	assert.NoError(s.T(), err, "expected no error for setting index read-only")

	settings, err := s.ec.IndexGetSettings(testOldIndexName).Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for getting index settings")

	indexBlocksSettings, found := settings[testOldIndexName].Settings["index"].(map[string]interface{})["blocks"]
	assert.True(s.T(), found, "index settings should have a blocks property")
	indexReadOnly, found := indexBlocksSettings.(map[string]interface{})["write"]
	assert.True(s.T(), found, "index blocks settings should have a write property")
	readOnly, _ := strconv.ParseBool(indexReadOnly.(string))
	assert.True(s.T(), readOnly, "index should be read-only")
}

func (s *EsServiceTestSuite) TestSetReadOnlyFailure() {
	s.service = esService{}
	s.forCurrentIndexVersion()

	err := s.service.setReadOnly(s.ec, testNewIndexName)
	assert.Error(s.T(), err, "expected error for setting index read-only")
	assert.Regexp(s.T(), "no such index", err.Error(), "error message")

	settings, err := s.ec.IndexGetSettings(testOldIndexName).Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for getting index settings")

	if indexBlocksSettings, found := settings[testOldIndexName].Settings["index"].(map[string]interface{})["blocks"]; found {
		if indexReadOnly, found := indexBlocksSettings.(map[string]interface{})["write"]; found {
			assert.True(s.T(), found, "index blocks settings should have a write property")
			readOnly, _ := strconv.ParseBool(indexReadOnly.(string))
			assert.False(s.T(), readOnly, "index should not be read-only")
		}
	}
}

func (s *EsServiceTestSuite) TestReindexAndWait() {
	s.service = esService{}
	s.forNextIndexVersion()
	err := createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for creating new index")

	count, err := s.service.reindex(s.ec, testOldIndexName, testNewIndexName)
	assert.NoError(s.T(), err, "expected no error for starting reindex")

	complete, done, err := s.service.isTaskComplete(s.ec, testNewIndexName, count)
	assert.NoError(s.T(), err, "expected no error for monitoring task completion")
	assert.Equal(s.T(), size, count, "index size")

	if !complete {
		assert.True(s.T(), done < count, "not all documents have been reindexed yet")

		// 100 documents may not reindex immediately but should only take a few seconds
		time.Sleep(5 * time.Second)
		complete, done, err = s.service.isTaskComplete(s.ec, testNewIndexName, count)
		assert.NoError(s.T(), err, "expected no error for monitoring task completion")
		assert.True(s.T(), complete, "expected reindex to be complete")
	}
	assert.Equal(s.T(), size, done, "all documents have been reindexed")

	actual, err := s.ec.Count(testNewIndexName).Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for checking index size")
	assert.Equal(s.T(), size, int(actual), "expected new index to contain same number of documents as original index")
}

func (s *EsServiceTestSuite) TestReindexFailure() {
	s.service = esService{}
	s.forNextIndexVersion()

	count, err := s.service.reindex(s.ec, testOldIndexName, testNewIndexName)
	assert.Error(s.T(), err, "expected error for starting reindex")
	assert.Regexp(s.T(), "no such index", err.Error(), "error message")
	assert.Equal(s.T(), 0, count, "index size")
}

func (s *EsServiceTestSuite) TestUpdateAlias() {
	s.service = esService{}
	s.forNextIndexVersion()
	err := createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for creating new index")

	err = createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	err = s.service.updateAlias(s.ec, testIndexName, "", testOldIndexName, testNewIndexName)
	assert.NoError(s.T(), err, "expected no error for updating alias")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")
}

func (s *EsServiceTestSuite) TestUpdateAliasNoIndexToRemove() {
	s.service = esService{}
	s.forNextIndexVersion()
	err := createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for creating new index")

	err = s.service.updateAlias(s.ec, testIndexName, "", "", testNewIndexName)
	assert.NoError(s.T(), err, "expected no error for updating alias")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	require.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")
}

func (s *EsServiceTestSuite) TestUpdateAliasFailure() {
	s.service = esService{}
	s.forNextIndexVersion()

	err := createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	err = s.service.updateAlias(s.ec, testIndexName, "", testOldIndexName, testNewIndexName)
	assert.Error(s.T(), err, "expected error for updating alias")
	assert.Regexp(s.T(), "no such index", err.Error(), "error message")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testOldIndexName, actual[0], "unmodified alias")
}

func (s *EsServiceTestSuite) TestUpdateAliasWithFilter() {
	s.service = esService{}
	s.forNextIndexVersion()
	err := createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for creating new index")

	err = createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	filter, err := ioutil.ReadFile(testAliasFilterFile)
	assert.NoError(s.T(), err, "this test case requires a query filter json at '%v'", testAliasFilterFile)

	err = s.service.updateAlias(s.ec, testIndexName, string(filter), testOldIndexName, testNewIndexName)
	assert.NoError(s.T(), err, "expected no error for updating alias")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")
}

func (s *EsServiceTestSuite) TestMigrateIndex() {
	s.service = esService{}
	s.forNextIndexVersion()

	_, err := s.ec.IndexPutSettings().BodyJson(map[string]interface{}{"index.number_of_replicas": 0}).Do(context.Background())
	require.NoError(s.T(), err, "expected no error in modifying replica settings")

	err = createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	s.service.elasticClient = s.ec
	s.service.pollReindexInterval = time.Second
	s.service.aliasName = testIndexName
	s.service.aliasForAllConcepts = aliasForAllConcepts
	s.service.mappingFile = testNewMappingFile
	err = s.service.MigrateIndex()

	assert.NoError(s.T(), err, "expected no error for migrating index in unhealthy ES cluster")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")

	actual = aliases.IndicesByAlias(aliasForAllConcepts)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")

	count, err := s.ec.Count(testNewIndexName).Do(context.Background())
	assert.Equal(s.T(), size, int(count), "new index size")

	count, err = s.ec.Count(testIndexName).Do(context.Background())
	assert.Equal(s.T(), size, int(count), "aliased index size")
}

func (s *EsServiceTestSuite) TestMigrateIndexWithAliasFilter() {
	s.service = esService{}
	s.forNextIndexVersion()

	_, err := s.ec.IndexPutSettings().BodyJson(map[string]interface{}{"index.number_of_replicas": 0}).Do(context.Background())
	require.NoError(s.T(), err, "expected no error in modifying replica settings")

	err = createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	s.service.elasticClient = s.ec
	s.service.pollReindexInterval = time.Second
	s.service.aliasName = testIndexName
	s.service.aliasForAllConcepts = aliasForAllConcepts
	s.service.mappingFile = testNewMappingFile
	s.service.aliasFilterFile = testAliasFilterFile
	err = s.service.MigrateIndex()

	assert.NoError(s.T(), err, "expected no error for migrating index in unhealthy ES cluster")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")

	actual = aliases.IndicesByAlias(aliasForAllConcepts)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")

	count, err := s.ec.Count(testNewIndexName).Do(context.Background())
	assert.Equal(s.T(), size, int(count), "new index size")

	count, err = s.ec.Count(testIndexName).Do(context.Background())
	assert.Equal(s.T(), size/2, int(count), "aliased index size")
}

func (s *EsServiceTestSuite) TestMigrateIndexWithMissingAliasFilter() {
	s.service = esService{}
	s.forNextIndexVersion()

	_, err := s.ec.IndexPutSettings().BodyJson(map[string]interface{}{"index.number_of_replicas": 0}).Do(context.Background())
	require.NoError(s.T(), err, "expected no error in modifying replica settings")

	err = createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	s.service.elasticClient = s.ec
	s.service.pollReindexInterval = time.Second
	s.service.aliasName = testIndexName
	s.service.mappingFile = testNewMappingFile
	s.service.aliasFilterFile = "./no-such-file.json"
	err = s.service.MigrateIndex()

	assert.Error(s.T(), err, "expected error for migrating index with missing alias filter")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testOldIndexName, actual[0], "unmodified alias")

	actual = aliases.IndicesByAlias(aliasForAllConcepts)
	assert.Len(s.T(), actual, 0, "aliases")
}

func (s *EsServiceTestSuite) TestMigrateIndexClusterUnhealthy() {
	s.service = esService{}
	s.forNextIndexVersion()

	err := createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	s.service.elasticClient = s.ec
	s.service.aliasName = testIndexName
	s.service.mappingFile = testNewMappingFile
	s.service.aliasFilterFile = testAliasFilterFile
	err = s.service.MigrateIndex()

	assert.Error(s.T(), err, "expected error for migrating index in unhealthy ES cluster")
	assert.EqualError(s.T(), err, "Cluster is yellow", "error message")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testOldIndexName, actual[0], "unmodified alias")
}

func (s *EsServiceTestSuite) TestMappingsCheckerInProgress() {
	s.service = esService{}
	s.forNextIndexVersion()
	progress := "some progress"
	s.service.progress = progress

	msg, err := s.service.mappingsChecker()
	assert.Contains(s.T(), msg, s.service.indexVersion, "healthcheck message")
	assert.Contains(s.T(), msg, progress, "healthcheck message")
	assert.Error(s.T(), err, "expected an unhealthy response")
}

func (s *EsServiceTestSuite) TestMappingsCheckerHealthy() {
	s.service = esService{}
	s.forNextIndexVersion()

	s.service.migrationCheck = true
	s.service.migrationErr = nil

	msg, err := s.service.mappingsChecker()
	assert.Regexp(s.T(), s.service.indexVersion, msg, "healthcheck message")
	assert.NoError(s.T(), err, "expected no error")
}

func (s *EsServiceTestSuite) TestMappingsCheckerUnhealthy() {
	s.service = esService{}
	s.forNextIndexVersion()

	expectedError := errors.New("test error")
	s.service.migrationCheck = true
	s.service.migrationErr = expectedError

	msg, err := s.service.mappingsChecker()
	assert.Equal(s.T(), "Elasticsearch mappings were not migrated successfully", msg, "healthcheck message")
	assert.EqualError(s.T(), err, expectedError.Error(), "expected error")
}

func hasMentionsCompletionMapping(mapping map[string]interface{}) bool {
	for _, v := range mapping {
		for _, fields := range v.(map[string]interface{})["mappings"].(map[string]interface{}) {
			prefLabel := fields.(map[string]interface{})["mapping"].(map[string]interface{})["prefLabel"].(map[string]interface{})
			if _, hasFields := prefLabel["fields"]; hasFields {
				if _, hasCompletion := prefLabel["fields"].(map[string]interface{})["mentionsCompletion"]; hasCompletion {
					return true
				}
			}
		}
	}

	return false
}
