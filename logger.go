package main

import (
	"github.com/hashicorp/go-hclog"
	golog "github.com/ipfs/go-log/v2"
	"go.uber.org/zap/zapcore"
	"io"
	"log"
	"os"
	"strings"
)

const systemName = "raft"

var raftLogger = golog.Logger(systemName)

type Logger interface {
	hclog.Logger
}

type consensusLogger struct {
	l                               *golog.ZapEventLogger
	name                            string
	decodeCount, failedRequestCount int
}

func defaultConsensusLogger() *consensusLogger {
	return &consensusLogger{
		l: raftLogger,
	}
}

func (c *consensusLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	lvl, _ := zapcore.ParseLevel(strings.TrimSpace(strings.ToLower(level.String())))
	c.l.Logln(lvl, msg, args)
}

func (c *consensusLogger) Trace(msg string, args ...interface{}) {
	c.l.Debugln(msg, args)
}

func (c *consensusLogger) Debug(msg string, args ...interface{}) {
	c.l.Debugln(msg, args)
}

func (c *consensusLogger) Info(msg string, args ...interface{}) {
	c.l.Infoln(msg, args)
}

func (c *consensusLogger) Warn(msg string, args ...interface{}) {
	c.l.Warnln(msg, args)
}

func (c *consensusLogger) Error(msg string, args ...interface{}) {
	if strings.Contains(msg, "failed to take snapshot") {
		return
	}
	if strings.Contains(msg, "failed to make requestVote RPC") && c.failedRequestCount < 10 {
		c.failedRequestCount++
		return
	}
	if strings.Contains(msg, "failed to heartbeat") && c.failedRequestCount < 10 {
		c.failedRequestCount++
		return
	}

	if strings.Contains(msg, "failed to decode incoming command") && c.decodeCount < 3 {
		c.decodeCount++
		return
	}
	c.l.Errorln(msg, args)
	c.decodeCount = 0
	c.failedRequestCount = 0
}

func (c *consensusLogger) IsTrace() bool {
	return c.l.Level() == zapcore.DebugLevel
}

func (c *consensusLogger) IsDebug() bool {
	return c.l.Level() == zapcore.DebugLevel
}

func (c *consensusLogger) IsInfo() bool {
	return c.l.Level() == zapcore.InfoLevel
}

func (c *consensusLogger) IsWarn() bool {
	return c.l.Level() == zapcore.WarnLevel
}

func (c *consensusLogger) IsError() bool {
	return c.l.Level() == zapcore.ErrorLevel
}

func (c *consensusLogger) ImpliedArgs() []interface{} {
	return []interface{}{}
}

func (c *consensusLogger) With(args ...interface{}) hclog.Logger {
	gl := golog.Logger(systemName)
	gl.SugaredLogger = *gl.With(args)

	return &consensusLogger{l: gl, name: c.name}
}

func (c *consensusLogger) Name() string {
	return c.name
}

func (c *consensusLogger) Named(name string) hclog.Logger {
	c.l.Named(name)
	c.name = name
	return c
}

func (c *consensusLogger) ResetNamed(name string) hclog.Logger {
	c.name = name
	return c
}

func (c *consensusLogger) SetLevel(level hclog.Level) {
	//_ = golog.SetLogLevel(systemName, strings.TrimSpace(strings.ToLower(level.String())))
}

func (c *consensusLogger) GetLevel() hclog.Level {
	lvl := c.l.Level()
	return hclog.LevelFromString(lvl.String())
}

func (c *consensusLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(os.Stderr, c.name, log.LstdFlags)
}

func (c *consensusLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return os.Stderr
}
