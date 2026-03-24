package cli

import (
	"time"
)

var CLI struct {
	Path               string        `default:"/api/v1/read" help:"Proxy endpoint path."`
	Healthcheck        string        `default:"/health" help:"Proxy health check path."`
	Metrics            string        `default:"/metrics" help:"Proxy metrics endpoint path."`
	Port               int           `default:"9091" help:"Proxy listen port."`
	HighThreshold      time.Duration `default:"24h" help:"Query range threshold for switching from raw time series endpoint to 1 minute aggregate endpoint."`
	NormalThreshold    time.Duration `default:"48h" help:"Query range threshold for switching from 1 minute aggregate endpoint to 5 minute aggregate endpoint."`
	LowThreshold       time.Duration `default:"120h" help:"Query range threshold for switching from 5 minute aggregate endpoint to 1 hour aggregate endpoint."`
	TargetRaw          string        `default:"http://localhost:9363/remote-read/raw" help:"Un-aggregated time series remote read target. Ignored for query-bypass."`
	TargetHigh         string        `default:"http://localhost:9363/remote-read/1m" help:"1 minute aggregate remote read target. Ignored for query-bypass."`
	TargetNormal       string        `default:"http://localhost:9363/remote-read/5m" help:"5 minute aggregate remote read target. Ignored for query-bypass."`
	TargetLow          string        `default:"http://localhost:9363/remote-read/1h" help:"1 hour aggregate remote read target. Ignored for query-bypass."`
	LogLevel           string        `enum:"debug,info,warn,error" default:"info" help:"Log level."`
	DebugLogSamples    bool          `default:"false" negatable:"" help:"Log sample values at debug log level"`
	QueryBypass        bool          `default:"true" negatable:"" help:"translate promQL queries directly to ClickHouse SQL instead of going through prometheus handler"`
	ChiUrl             string        `default:"localhost:9000" help:"ClickHouse URL and port. Ignored for no-query-bypass."`
	ChiTsdb            string        `default:"timeseries_db" help:"ClickHouse timeseries database name. Ignored for no-query-bypass."`
	ChiUser            string        `default:"ingest" help:"ClickHouse timeseries database user. Ignored for no-query-bypass."`
	ChiPassword        string        `help:"ClickHouse timeseries database name user password. Ignored for no-query-bypass."`
	ChiTableRaw        string        `default:"timeseries_data_table" help:"Un-aggregated time series table. Ignored for no-query-bypass."`
	ChiTableHigh       string        `default:"timeseries_1m_table" help:"1 minute aggregate table. Ignored for no-query-bypass."`
	ChiTableNormal     string        `default:"timeseries_5m_table" help:"5 minute aggregate table. Ignored for no-query-bypass."`
	ChiTableLow        string        `default:"timeseries_1h_table" help:"1 hour aggregate table. Ignored for no-query-bypass."`
	ChiMaxQuery        int           `default:"300" help:"Max clickhouse query execution time in seconds. Ignored for no-query-bypass."`
	ChiDialTimeout     time.Duration `default:"10s" help:"ClickHouse dial timeout. Ignored for no-query-bypass."`
	ChiMaxOpenConns    int           `default:"10" help:"ClickHouse max open connections. Ignored for no-query-bypass."`
	ChiMaxIdleConns    int           `default:"5" help:"ClickHouse max idle connections. Ignored for no-query-bypass."`
	ChiQueryMaxThreads int           `default:"8" help:"ClickHouse query max_threads setting value. Ignored for no-query-bypass."`
	ChiConnLifetime    time.Duration `default:"1h" help:"ClickHouse max connection lifetime. Ignored for no-query-bypass."`
}
