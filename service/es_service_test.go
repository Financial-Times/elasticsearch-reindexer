package service

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/olivere/elastic.v5"
)

const (
	apiBaseURL         = "http://test.api.ft.com"
	testIndexName      = "test-index"
	testIndexVersion   = "0.0.1"
	esTopicType        = "topics"
	ftTopicType        = "http://www.ft.com/ontology/Topic"
	testOldMappingFile = "test/old-mapping.json"
	testNewMappingFile = "test/new-mapping.json"
	size               = 100
)

var (
	testOldIndexName string
	testNewIndexName string
)

func getElasticSearchTestURL(t *testing.T) string {
	esURL := os.Getenv("ELASTICSEARCH_TEST_URL")
	if strings.TrimSpace(esURL) == "" {
		t.Fatal("Please set the environment variable ELASTICSEARCH_TEST_URL to run ElasticSearch integration tests (e.g. export ELASTICSEARCH_TEST_URL=http://localhost:9200).")
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
	log.WithFields(log.Fields{"index": indexName, "alias": aliasName}).Info("test case is creating alias")
	_, err := ec.Alias().Add(indexName, aliasName).Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func writeTestConcepts(ec *elastic.Client, indexName string, esConceptType string, ftConceptType string, amount int) error {
	for i := 0; i < amount; i++ {
		uuid := uuid.NewV4().String()

		payload := map[string]interface{}{
			"id":         uuid,
			"apiUrl":     fmt.Sprintf("%s/%s/%s", apiBaseURL, esConceptType, uuid),
			"prefLabel":  fmt.Sprintf("Test concept %s %s", esConceptType, uuid),
			"types":      []string{ftConceptType},
			"directType": ftConceptType,
			"aliases":    []string{},
		}

		_, err := ec.Index().
			Index(indexName).
			Type(esConceptType).
			Id(uuid).
			BodyJson(payload).
			Do(context.Background())
		if err != nil {
			return err
		}
	}

	// ensure test data is immediately available from the index
	_, err := ec.Refresh(testIndexName).Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

type EsServiceCheckIndexAliasTestSuite struct {
	suite.Suite
	esURL     string
	ec        *elastic.Client
	indexName string
	service   esService
}

func TestEsServiceCheckIndexAliasSuite(t *testing.T) {
	suite.Run(t, new(EsServiceCheckIndexAliasTestSuite))
}

func (s *EsServiceCheckIndexAliasTestSuite) SetupSuite() {
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

func (s *EsServiceCheckIndexAliasTestSuite) SetupTest() {
	s.indexName = testOldIndexName

	s.ec.Alias().Remove(testOldIndexName, testIndexName).Do(context.Background())
	s.ec.Alias().Remove(testNewIndexName, testIndexName).Do(context.Background())
	s.ec.DeleteIndex(testOldIndexName).Do(context.Background())
	s.ec.DeleteIndex(testNewIndexName).Do(context.Background())

	err := createIndex(s.ec, testOldIndexName, testOldMappingFile)
	require.NoError(s.T(), err, "expected no error in creating index")

	err = createAlias(s.ec, testIndexName, testOldIndexName)
	require.NoError(s.T(), err, "expected no error in creating index alias")

	err = writeTestConcepts(s.ec, testOldIndexName, esTopicType, ftTopicType, size)
	require.NoError(s.T(), err, "expected no error in adding topics")
}

func forCurrentIndexVersion() {
	indexVersion = testIndexVersion
}

func forNextIndexVersion() {
	oldVersion := semver.MustParse(testIndexVersion)
	requiredVersion := oldVersion.IncPatch()
	indexVersion = requiredVersion.String()
}

func (s *EsServiceCheckIndexAliasTestSuite) TestCheckIndexAliasesMatch() {
	forCurrentIndexVersion()

	requireUpdate, current, required, err := s.service.checkIndexAliases(s.ec, testIndexName)

	assert.NoError(s.T(), err, "expected no error for checking index")
	assert.False(s.T(), requireUpdate, "expected no update required")
	assert.Equal(s.T(), testOldIndexName, current, "current index")
	assert.Equal(s.T(), testOldIndexName, required, "required index")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestCheckIndexAliasesDoNotMatch() {
	forNextIndexVersion()

	requireUpdate, current, required, err := s.service.checkIndexAliases(s.ec, testIndexName)

	assert.NoError(s.T(), err, "expected no error for checking index")
	assert.True(s.T(), requireUpdate, "expected update required")
	assert.Equal(s.T(), testOldIndexName, current, "current index")
	assert.Equal(s.T(), testNewIndexName, required, "required index")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestCheckIndexAliasesNotFound() {
	forCurrentIndexVersion()

	requireUpdate, _, _, err := s.service.checkIndexAliases(s.ec, "test-missing")

	assert.EqualError(s.T(), err, ErrInvalidAlias.Error(), "expected no error for checking index")
	assert.False(s.T(), requireUpdate, "expected no update required")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestCreateIndex() {
	forNextIndexVersion()
	indexMapping, err := ioutil.ReadFile(testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for reading index mapping file")

	err = s.service.createIndex(s.ec, testNewIndexName, string(indexMapping))

	assert.NoError(s.T(), err, "expected no error for creating index")

	mapping, err := s.ec.GetFieldMapping().Index(testNewIndexName).Field("prefLabel").Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for reading index mapping")
	assert.True(s.T(), hasMentionsCompletionMapping(mapping), "new index should have mentionsCompletion in its mappings")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestCreateIndexFailure() {
	forNextIndexVersion()
	indexMapping, err := ioutil.ReadFile(testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for reading index mapping file")

	err = s.service.createIndex(s.ec, testOldIndexName, string(indexMapping))
	assert.Error(s.T(), err, "expected error for creating index")
	assert.Regexp(s.T(), fmt.Sprintf("index.+%s.+already exists", regexp.QuoteMeta(testOldIndexName)), err.Error(), "error message")

	oldMapping, err := s.ec.GetFieldMapping().Index(testOldIndexName).Field("prefLabel").Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for reading index mapping")
	assert.False(s.T(), hasMentionsCompletionMapping(oldMapping), "old index should not have mentionsCompletion in its mappings")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestSetReadOnly() {
	forCurrentIndexVersion()

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

func (s *EsServiceCheckIndexAliasTestSuite) TestSetReadOnlyFailure() {
	forCurrentIndexVersion()

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

func (s *EsServiceCheckIndexAliasTestSuite) TestReindexAndWait() {
	forNextIndexVersion()
	err := createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for creating new index")

	count, err := s.service.reindex(s.ec, testOldIndexName, testNewIndexName)
	assert.NoError(s.T(), err, "expected no error for starting reindex")

	complete, err := s.service.isTaskComplete(s.ec, testNewIndexName, count)
	assert.NoError(s.T(), err, "expected no error for monitoring task completion")
	assert.Equal(s.T(), size, count, "index size")

	if !complete {
		// 100 documents may not reindex immediately but should only take a few seconds
		time.Sleep(5 * time.Second)
		complete, err := s.service.isTaskComplete(s.ec, testNewIndexName, count)
		assert.NoError(s.T(), err, "expected no error for monitoring task completion")
		assert.True(s.T(), complete, "expected reindex to be complete")
	}

	actual, err := s.ec.Count(testNewIndexName).Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for checking index size")
	assert.Equal(s.T(), size, int(actual), "expected new index to contain same number of documents as original index")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestReindexFailure() {
	forNextIndexVersion()

	count, err := s.service.reindex(s.ec, testOldIndexName, testNewIndexName)
	assert.Error(s.T(), err, "expected error for starting reindex")
	assert.Regexp(s.T(), "no such index", err.Error(), "error message")
	assert.Equal(s.T(), 0, count, "index size")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestUpdateAlias() {
	forNextIndexVersion()
	err := createIndex(s.ec, testNewIndexName, testNewMappingFile)
	require.NoError(s.T(), err, "expected no error for creating new index")

	err = s.service.updateAlias(s.ec, testIndexName, testOldIndexName, testNewIndexName)
	assert.NoError(s.T(), err, "expected no error for updating alias")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testNewIndexName, actual[0], "updated alias")
}

func (s *EsServiceCheckIndexAliasTestSuite) TestUpdateAliasFailure() {
	forNextIndexVersion()

	err := s.service.updateAlias(s.ec, testIndexName, testOldIndexName, testNewIndexName)
	assert.Error(s.T(), err, "expected error for updating alias")
	assert.Regexp(s.T(), "no such index", err.Error(), "error message")

	aliases, err := s.ec.Aliases().Do(context.Background())
	assert.NoError(s.T(), err, "expected no error for retrieving aliases")

	actual := aliases.IndicesByAlias(testIndexName)
	assert.Len(s.T(), actual, 1, "aliases")
	assert.Equal(s.T(), testOldIndexName, actual[0], "unmodified alias")
}

func hasMentionsCompletionMapping(mapping map[string]interface{}) bool {
	for _, v := range mapping {
		for _, fields := range v.(map[string]interface{})["mappings"].(map[string]interface{}) {
			prefLabel := fields.(map[string]interface{})["prefLabel"].(map[string]interface{})["mapping"].(map[string]interface{})["prefLabel"].(map[string]interface{})
			if _, hasFields := prefLabel["fields"]; hasFields {
				if _, hasCompletion := prefLabel["fields"].(map[string]interface{})["mentionsCompletion"]; hasCompletion {
					return true
				}
			}
		}
	}

	return false
}
