package main

import (
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/elasticsearch-reindexer/service"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	log "github.com/Financial-Times/go-logger"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/husobee/vestigo"
	cli "github.com/jawher/mow.cli"
	"github.com/olivere/elastic/v7"
)

func main() {
	app := cli.App("elasticsearch-reindexer", "ElasticSearch reindexer")
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	esEndpoint := app.String(cli.StringOpt{
		Name:   "elasticsearch-endpoint",
		Value:  "http://localhost:9200",
		Desc:   "ES endpoint",
		EnvVar: "ELASTICSEARCH_ENDPOINT",
	})
	esRegion := app.String(cli.StringOpt{
		Name:   "elasticsearch-region",
		Value:  "eu-west-1",
		Desc:   "ES region",
		EnvVar: "ELASTICSEARCH_REGION",
	})
	esAuth := app.String(cli.StringOpt{
		Name:   "auth",
		Value:  "none",
		Desc:   "Authentication method for ES cluster (aws or none)",
		EnvVar: "AUTH",
	})
	esIndex := app.String(cli.StringOpt{
		Name:   "elasticsearch-index-alias",
		Value:  "concepts",
		Desc:   "Elasticsearch index alias",
		EnvVar: "ELASTICSEARCH_INDEX_ALIAS",
	})
	mappingVersion := app.String(cli.StringOpt{
		Name:   "mapping-version",
		Value:  "",
		Desc:   "Mapping file / index version",
		EnvVar: "INDEX_VERSION",
	})
	mappingFile := app.String(cli.StringOpt{
		Name:   "mapping-file",
		Value:  "./mapping.json",
		Desc:   "Mapping file",
		EnvVar: "MAPPING_FILE",
	})
	aliasFilterFile := app.String(cli.StringOpt{
		Name:   "alias-filter-file",
		Value:  "",
		Desc:   "An optional filter query to apply to the alias",
		EnvVar: "ALIAS_FILTER_FILE",
	})
	aliasForAllConcepts := app.String(cli.StringOpt{
		Name:   "alias-for-all-concepts",
		Value:  "all-concepts",
		Desc:   "The name of the index alias which won't have any filters",
		EnvVar: "ALIAS_FOR_ALL_CONCEPTS",
	})
	esTraceLogging := app.Bool(cli.BoolOpt{
		Name:   "elasticsearch-trace",
		Value:  false,
		Desc:   "Whether to log ElasticSearch HTTP requests and responses",
		EnvVar: "ELASTICSEARCH_TRACE",
	})
	systemCode := app.String(cli.StringOpt{
		Name:   "system-code",
		Value:  "NO-SYSTEM-CODE",
		Desc:   "System code",
		EnvVar: "SYSTEM_CODE",
	})
	panicGuideUrl := app.String(cli.StringOpt{
		Name:   "panic-guide-url",
		Value:  "https://runbooks.in.ft.com/concepts-reindexer",
		Desc:   "Panic Guide URL",
		EnvVar: "PANIC_GUIDE_URL",
	})

	log.InitDefaultLogger("elasticsearch-reindexer")

	app.Action = func() {
		logStartupConfig(port, esEndpoint, esAuth, esIndex, esRegion)

		awsSession, err := session.NewSession()
		if err != nil {
			log.WithError(err).Fatal("Failed to initialize AWS session")
		}
		credValues, err := awsSession.Config.Credentials.Get()
		if err != nil {
			log.WithError(err).Fatal("Failed to obtain AWS credentials values")
		}
		log.Infof("Obtaining AWS credentials by using [%s] as provider", credValues.ProviderName)
		accessConfig := service.NewAccessConfig(awsSession.Config.Credentials, *esRegion, *esEndpoint, *esAuth, *esTraceLogging)

		// It seems that once we have a connection, we can lose and reconnect to Elastic OK
		// so just keep going until successful
		ecc := make(chan *elastic.Client)
		go func() {
			defer close(ecc)
			for {
				ec, err := service.NewElasticClient(accessConfig)
				if err == nil {
					log.Info("connected to ElasticSearch")
					ecc <- ec
					return
				} else {
					log.WithError(err).Error("could not connect to ElasticSearch")
					time.Sleep(time.Minute)
				}
			}
		}()

		esService := service.NewEsService(ecc, *esIndex, *mappingFile, *aliasFilterFile, *mappingVersion, *panicGuideUrl, *aliasForAllConcepts)
		routeRequest(port, esService, *systemCode)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func logStartupConfig(port, esEndpoint, esAuth, esIndex, esRegion *string) {
	log.Info("ElasticSearch reindexer uses the following configuration:")
	log.Infof("port: %v", *port)
	log.Infof("elasticsearch-endpoint: %v", *esEndpoint)
	log.Infof("elasticsearch-auth: %v", *esAuth)
	log.Infof("elasticsearch-index: %v", *esIndex)
	log.Infof("elasticsearch-region: %v", *esRegion)
}

func routeRequest(port *string, healthService service.EsHealthService, systemCode string) {
	servicesRouter := vestigo.NewRouter()

	healthCheck := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  systemCode,
			Name:        "Elasticsearch Service Healthcheck",
			Description: "Checks for ES",
			Checks: []fthealth.Check{
				healthService.ConnectivityHealthyCheck(),
				healthService.ClusterIsHealthyCheck(),
				healthService.IndexMappingsCheck(),
			},
		},
		Timeout: 10 * time.Second,
	}
	http.HandleFunc("/__health", fthealth.Handler(healthCheck))

	http.HandleFunc(status.GTGPath, status.NewGoodToGoHandler(healthService.GTG))
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	http.Handle("/", servicesRouter)

	log.Infof("ElasticSearch reindexer listening on port %v...", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatalf("Unable to start: %v", err)
	}
}
