package main

import (
	"net/http"
	"os"

	"github.com/alecthomas/rapid/schema"

	"github.com/alecthomas/util"

	"github.com/alecthomas/kingpin"
	"github.com/alecthomas/tuplespace/service"
)

var (
	bindFlag = kingpin.Flag("bind", "Bind address for service.").Default("127.0.0.1:2619").TCP()
	ramlFlag = kingpin.Flag("raml", "Dump RAML service definition.").Bool()
	goFlag   = kingpin.Flag("go", "Generate Go client code.").Hidden().Bool()
)

func main() {
	util.Bootstrap(kingpin.CommandLine, util.AllModules, nil)
	if *ramlFlag {
		err := schema.SchemaToRAML("http://"+(*bindFlag).String(), service.Definition(), os.Stdout)
		kingpin.FatalIfError(err, "failed to generate RAML service definition")
		return
	}
	if *goFlag {
		err := schema.SchemaToGoClient(service.Definition(), true, "github.com/alecthomas/tuplespace", os.Stdout)
		kingpin.FatalIfError(err, "failed to generate Go client")
		return
	}
	server, err := service.Server()
	kingpin.FatalIfError(err, "failed to create new server")
	err = http.ListenAndServe((*bindFlag).String(), server)
	kingpin.FatalIfError(err, "server failed")
}
