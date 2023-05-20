module github.com/piyushsingariya/syndicate

go 1.19

require (
	github.com/invopop/jsonschema v0.7.0
	github.com/spf13/cobra v1.7.0
	sigs.k8s.io/yaml v1.3.0
)

require github.com/stretchr/testify v1.8.1 // indirect

require (
	github.com/iancoleman/orderedmap v0.0.0-20190318233801-ac98e3ecb4b0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/invopop/jsonschema v0.7.0 => ../../jsonschema
