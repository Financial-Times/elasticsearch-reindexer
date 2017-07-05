package main

import (
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/elasticsearch-reindexer/service"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/husobee/vestigo"
	"github.com/jawher/mow.cli"
	"gopkg.in/olivere/elastic.v5"
)

func main() {
	app := cli.App("elasticsearch-reindexer", "ElasticSearch reindexer")
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	accessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key",
		Desc:   "AWS ACCESS KEY",
		EnvVar: "AWS_ACCESS_KEY_ID",
	})
	secretKey := app.String(cli.StringOpt{
		Name:   "aws-secret-access-key",
		Desc:   "AWS SECRET ACCESS KEY",
		EnvVar: "AWS_SECRET_ACCESS_KEY",
	})
	esEndpoint := app.String(cli.StringOpt{
		Name:   "elasticsearch-endpoint",
		Value:  "http://localhost:9200",
		Desc:   "ES endpoint",
		EnvVar: "ELASTICSEARCH_ENDPOINT",
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
	mappingFile := app.String(cli.StringOpt{
		Name:   "mapping-file",
		Value:  "./mapping.json",
		Desc:   "Mapping file",
		EnvVar: "MAPPING_FILE",
	})
	esTraceLogging := app.Bool(cli.BoolOpt{
		Name:   "elasticsearch-trace",
		Value:  false,
		Desc:   "Whether to log ElasticSearch HTTP requests and responses",
		EnvVar: "ELASTICSEARCH_TRACE",
	})

	log.SetLevel(log.InfoLevel)

	app.Action = func() {
		logStartupConfig(port, esEndpoint, esAuth, esIndex)

		accessConfig := service.NewAccessConfig(*esEndpoint, *esTraceLogging, *esAuth, *accessKey, *secretKey)

		// It seems that once we have a connection, we can lose and reconnect to Elastic OK
		// so just keep going until successful
		ecc := make(chan *elastic.Client)
		go func() {
			defer close(ecc)
			for {
				ec, err := service.NewElasticClient(accessConfig)
				if err == nil {
					log.Infof("connected to ElasticSearch")
					ecc <- ec
					return
				} else {
					log.Errorf("could not connect to ElasticSearch: %s", err.Error())
					time.Sleep(time.Minute)
				}
			}
		}()

		esService := service.NewEsService(ecc, *esIndex, *mappingFile)
		routeRequest(port, esService)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Errorf("App could not start, error=[%s]\n", err)
		return
	}
}

func logStartupConfig(port, esEndpoint, esAuth, esIndex *string) {
	log.Info("ElasticSearch reindexer uses the following configuration:")
	log.Infof("port: %v", *port)
	log.Infof("elasticsearch-endpoint: %v", *esEndpoint)
	log.Infof("elasticsearch-auth: %v", *esAuth)
	log.Infof("elasticsearch-index: %v", *esIndex)
}

func routeRequest(port *string, healthService service.EsHealthService) {
	servicesRouter := vestigo.NewRouter()

	var monitoringRouter http.Handler = servicesRouter

	healthCheck := fthealth.HealthCheck{
		SystemCode:  "???",
		Name:        "Elasticsearch Service Healthcheck",
		Description: "Checks for ES",
		Checks: []fthealth.Check{
			healthService.ConnectivityHealthyCheck(),
			healthService.ClusterIsHealthyCheck(),
		},
	}
	http.HandleFunc("/__health", fthealth.Handler(healthCheck))

	http.HandleFunc(status.GTGPath, healthService.GoodToGo)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	http.Handle("/", monitoringRouter)

	log.Infof("ElasticSearch reindexer listening on port %v...", *port)
	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatalf("Unable to start: %v", err)
	}
}
