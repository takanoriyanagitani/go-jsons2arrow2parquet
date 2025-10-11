#!/bin/sh

set -e

# Function to generate sample JSON data
geninput(){
	jq -c -n '{
		"timestamp":"2025-10-06T00:19:40.012345Z",
		"unixtime": 1234567890,
		"severity":"INFO",
		"status":200,
		"value":299792458,
		"amount":42.195,
		"height":3.776,
		"method":"GET",
		"body":"apt update done",
		"id":"cafef00d-dead-beaf-face-864299792458",
		"active":true,
        "author": null
	}'

	jq -c -n '{
		"timestamp":"2025-10-07T00:19:40.012345Z",
		"unixtime": 1234567891,
		"severity":"WARN",
		"status":200,
		"value":16777216,
		"amount":42.195,
		"height": 0.599,
		"method":"GET",
		"author":"jd",
		"body":"apt update failure",
		"id":"cafef00d-dead-beaf-face-864299792458",
		"active":false
	}'
}

# Function to generate the Avro schema
genschema(){
	jq -n '{
		"type":"record",
		"name":"log",
		"fields":[
			{"name":"timestamp","type":"string"},
			{"name":"unixtime", "type": {
				"type":"long",
				"logicalType":"timestamp-micros"
			}},
			{"name":"severity","type":{
				"type":"enum",
				"name":"Severity",
				"symbols":[
					"TRACE",
					"DEBUG",
					"INFO",
					"WARN",
					"ERROR",
					"FATAL"
				]
			}},
			{"name":"status","type":"int"},
			{"name":"value","type":"long"},
			{"name":"amount","type":"float"},
			{"name":"height","type":"double"},
			{"name":"method","type":"string"},
			{"name":"active","type":"boolean"},
			{"name":"author","type":["null","string"]},
			{"name":"id","type":{
				"type":"string",
				"logicalType":"uuid"
			}},
			{"name":"body","type":"string"}
		]
	}'
}

# The name of the Avro schema file
AVSC_FILE="sample.avsc"

# The path to the CLI executable
CLI_APP="./jsons2arrow2parquet"

# Generate the schema file if it doesn't exist
if [ ! -f "${AVSC_FILE}" ]; then
    echo "--- Generating Avro schema file (${AVSC_FILE}) ---"
    genschema > "${AVSC_FILE}"
    echo "--- Schema file generated. ---"
else
    echo "--- Using existing schema file (${AVSC_FILE}) ---"
fi
echo ""

# Check if the CLI executable exists
if [ ! -f "${CLI_APP}" ]; then
    echo "Building the CLI app..."
    go build -o "${CLI_APP}" .
fi

echo "--- Running the conversion ---"
echo "Generating JSONs -> ${CLI_APP} -> parquet-read"
echo ""

opq="./output.parquet"

# Generate JSONs, convert to Parquet, and read with parquet-read
geninput | ./${CLI_APP} -avsc "${AVSC_FILE}" > "${opq}"

test -f "${opq}" && rsql --url "parquet://./${opq}" -- '
	SELECT
	  timestamp,
	  unixtime,
	  severity,
	  status,
	  value,
	  amount,
	  height,
	  method,
	  active,
	  author,
	  body
    FROM output
'
