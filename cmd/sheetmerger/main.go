package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/subcommands"
	"github.com/mix3/sheetmerger"
)

type mergeCmd struct {
	credential     string
	baseSheetKey   string
	diffSheetKey   string
	indexSheetName string
}

func (*mergeCmd) Name() string     { return "merge" }
func (*mergeCmd) Synopsis() string { return "" }
func (*mergeCmd) Usage() string {
	return ""
}

func (p *mergeCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.credential, "credential", "credential.json", "")
	f.StringVar(&p.baseSheetKey, "base-sheet-key", "", " (*) required")
	f.StringVar(&p.diffSheetKey, "diff-sheet-key", "", " (*) required")
	f.StringVar(&p.indexSheetName, "index-sheet-name", "table_map", "")

	f.VisitAll(func(f *flag.Flag) {
		envName := strings.Replace(strings.ToUpper(f.Name), "-", "_", -1)
		if s := os.Getenv(envName); s != "" {
			f.Value.Set(s)
		}
	})
}

func (m *mergeCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	sm, err := sheetmerger.NewSheetMerger(m.credential)
	if err != nil {
		log.Printf("%+v", err)
		return subcommands.ExitFailure
	}
	sm.IndexSheetName = m.indexSheetName

	err = sm.MergeBySheetKey(
		m.baseSheetKey,
		m.diffSheetKey,
		f.Args()...,
	)
	if err != nil {
		log.Printf("%+v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

type backupCmd struct {
	credential        string
	indexSheetKey     string
	baseFolderId      string
	srcFolderName     string
	dstBaseFolderName string
	indexSheetName    string
}

func (*backupCmd) Name() string     { return "backup" }
func (*backupCmd) Synopsis() string { return "" }
func (*backupCmd) Usage() string {
	return ""
}

func (p *backupCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&p.credential, "credential", "credential.json", "")
	f.StringVar(&p.indexSheetKey, "index-sheet-key", "", " (*) required")
	f.StringVar(&p.baseFolderId, "base-folder-id", "", " (*) required")
	f.StringVar(&p.dstBaseFolderName, "dst-base-folder-name", "backup", "")
	f.StringVar(&p.indexSheetName, "index-sheet-name", "table_map", "")

	f.VisitAll(func(f *flag.Flag) {
		envName := strings.Replace(strings.ToUpper(f.Name), "-", "_", -1)
		if s := os.Getenv(envName); s != "" {
			f.Value.Set(s)
		}
	})
}

func (b *backupCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	sm, err := sheetmerger.NewSheetMerger(b.credential)
	if err != nil {
		log.Printf("%+v", err)
		return subcommands.ExitFailure
	}
	sm.BackupFolderName = b.dstBaseFolderName
	sm.IndexSheetName = b.indexSheetName

	err = sm.Backup(
		b.indexSheetKey,
		b.baseFolderId,
		time.Now().Format("20060102150405"),
	)
	if err != nil {
		log.Printf("%+v", err)
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&mergeCmd{}, "")
	subcommands.Register(&backupCmd{}, "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
