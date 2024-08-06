package db

import (
	"go.opentelemetry.io/otel/attribute"
)

const InstrumName = "github.com/XiBao/db"

var RowsAffected = attribute.Key("db.rows_affected")
