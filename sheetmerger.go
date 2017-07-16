package sheetmerger

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/pkg/errors"

	drive "google.golang.org/api/drive/v3"
	sheets "google.golang.org/api/sheets/v4"

	"golang.org/x/oauth2/google"
)

type SheetMerger struct {
	client           *http.Client
	driveService     *drive.Service
	sheetsService    *sheets.Service
	BackupFolderName string
	IndexSheetName   string
}

func NewSheetMerger(credentialFilePath string) (*SheetMerger, error) {
	data, err := ioutil.ReadFile(credentialFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "read file failed. file:%s", credentialFilePath)
	}

	conf, err := google.JWTConfigFromJSON(
		data,
		drive.DriveScope,
		drive.DriveMetadataScope,
		sheets.SpreadsheetsScope,
	)
	if err != nil {
		return nil, errors.Wrap(err, "create jwtConfig failed.")
	}

	client := conf.Client(context.Background())

	driveService, err := drive.New(client)
	if err != nil {
		return nil, errors.Wrap(err, "create driveService failed.")
	}

	sheetsService, err := sheets.New(client)
	if err != nil {
		return nil, errors.Wrap(err, "create sheetsService failed.")
	}

	return &SheetMerger{
		client:           client,
		driveService:     driveService,
		sheetsService:    sheetsService,
		BackupFolderName: "backup",
		IndexSheetName:   "table_map",
	}, nil
}

func (sm *SheetMerger) Backup(indexSheetKey, baseFolderID, backupFolderName string) error {
	if indexSheetKey == "" {
		return fmt.Errorf("indexSheetKey is required.")
	}
	if baseFolderID == "" {
		return fmt.Errorf("baseFolderID is required.")
	}

	log.Println("start backup")

	// backup フォルダを探す
	folderSearch, err := sm.driveService.Files.List().Q(fmt.Sprintf(
		"'%s' in parents and mimeType = 'application/vnd.google-apps.folder'",
		baseFolderID,
	)).Do()
	if err != nil {
		return errors.Wrapf(
			err,
			"search failed: `'%s' in parents and mimeType = 'application/vnd.google-apps.folder'`",
			baseFolderID,
		)
	}

	var backupFolderID string
	for _, f := range folderSearch.Files {
		switch f.Name {
		case sm.BackupFolderName:
			backupFolderID = f.Id
		}
	}

	// backup フォルダ以下にバックアップ用のフォルダを掘る
	backupFolder, err := sm.driveService.Files.Create(&drive.File{
		Name:     backupFolderName,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{backupFolderID},
	}).Do()
	if err != nil {
		return errors.Wrapf(
			err,
			"create folder failed: parent:%s name:%s",
			backupFolderID, backupFolderName,
		)
	}

	indexSheet, err := sm.newSheet(indexSheetKey, sm.IndexSheetName)
	if err != nil {
		return err
	}

	// オリジナルの table_map から key のリスト取ってきてバックアップフォルダにコピー
	var backupIndexSheet *sheet
	replaceIDMap := map[string]string{}
	keys := indexSheet.uniqueValuesByColumn("key")
	for _, key := range append([]string{indexSheetKey}, keys...) {
		// コピー時にファイル名が欲しいので一旦 Get する
		file, err := sm.driveService.Files.Get(key).Do()
		if err != nil {
			return errors.Wrapf(err, "file fetch failed. key:%s", key)
		}

		// コピー
		backupFile, err := sm.driveService.Files.Copy(key, &drive.File{
			Name:    file.Name,
			Parents: []string{backupFolder.Id},
		}).Do()
		if err != nil {
			return errors.Wrapf(
				err,
				"file copy failed. from:key:%s to:parent:%s to:name:%s",
				key, backupFolder.Id, file.Name,
			)
		}

		log.Printf("%s : done", file.Name)

		if backupIndexSheet == nil {
			if sheet, err := sm.newSheet(backupFile.Id, sm.IndexSheetName); err == nil {
				backupIndexSheet = sheet
			}
		}

		replaceIDMap[key] = backupFile.Id
	}

	// バックアップした方の table_map の key をバックアップしたファイルの key に書き換える
	if err := backupIndexSheet.replaceByColumn("key", replaceIDMap); err != nil {
		return err
	}

	// table_map を書き換えたので同期を取る
	if err := backupIndexSheet.refresh(); err != nil {
		return err
	}

	// バックアップの table_map の key の更新に成功してるか元の table_map の key との重複チェックで確認
	dupCheckMap := map[string]struct{}{}
	for _, k := range backupIndexSheet.uniqueValuesByColumn("key") {
		dupCheckMap[k] = struct{}{}
	}
	for _, k := range indexSheet.uniqueValuesByColumn("key") {
		if _, ok := dupCheckMap[k]; ok {
			// 失敗してたらバックアップフォルダ消す
			err := sm.driveService.Files.Delete(backupFolder.Id).Do()
			if err != nil {
				return errors.Wrapf(err, "delete folder failed. id:%s", backupFolder.Id)
			}
			return fmt.Errorf("fail in duplicate")
		}
	}

	log.Println("finish backup")

	return nil
}

func (sm *SheetMerger) newSheet(key, sheetName string) (*sheet, error) {
	r, err := sm.sheetsService.Spreadsheets.Values.Get(key, sheetName).Do()
	if err != nil {
		return nil, errors.Wrapf(err, "fetch failed. key:%s sheetname:%s", key, sheetName)
	}

	s := &sheet{
		sheetKey:       key,
		sheetName:      sheetName,
		indexSheetName: sm.IndexSheetName,
		sheetsService:  sm.sheetsService,
		values:         r.Values,
	}
	return s, nil
}

func (sm *SheetMerger) MergeBySheetKey(baseSheetKey, diffSheetKey string, sheetNames ...string) error {
	if baseSheetKey == "" {
		return fmt.Errorf("baseSheetKey is required.")
	}
	if diffSheetKey == "" {
		return fmt.Errorf("diffSheetKey is required.")
	}

	baseIndexSheet, err := sm.newSheet(baseSheetKey, sm.IndexSheetName)
	if err != nil {
		return err
	}
	diffIndexSheet, err := sm.newSheet(diffSheetKey, sm.IndexSheetName)
	if err != nil {
		return err
	}
	for _, sheetName := range sheetNames {
		err = sm.mergeByIndexSheet(
			baseIndexSheet,
			diffIndexSheet,
			sheetName,
		)
		if err != nil {
			return errors.Wrapf(
				err,
				"sheet merge failed. base:%s diff:%s sheetName:%s",
				baseIndexSheet.sheetKey, diffIndexSheet.sheetKey, sheetName,
			)
		}
	}
	return nil
}

func (sm *SheetMerger) mergeByIndexSheet(baseIndexSheet, diffIndexSheet *sheet, sheetName string) error {
	if baseIndexSheet.sheetKey == diffIndexSheet.sheetKey {
		return fmt.Errorf("%s : same key", sheetName)
	}

	targetBaseIndexRows := []map[string]interface{}{}
	for _, row := range baseIndexSheet.rows() {
		if row["sheetname"].(string) == sheetName {
			targetBaseIndexRows = append(targetBaseIndexRows, row)
		}
	}
	if 1 < len(targetBaseIndexRows) {
		return fmt.Errorf("%s : sheetname duplication in base index ws", sheetName)
	} else if 0 == len(targetBaseIndexRows) {
		return fmt.Errorf("%s : no corresponding base index rows", sheetName)
	}

	targetDiffIndexRows := []map[string]interface{}{}
	for _, row := range diffIndexSheet.rows() {
		if row["sheetname"].(string) == sheetName {
			targetDiffIndexRows = append(targetDiffIndexRows, row)
		}
	}
	if 1 < len(targetDiffIndexRows) {
		return fmt.Errorf("%s : sheetname duplication in diff index ws", sheetName)
	} else if 0 == len(targetDiffIndexRows) {
		return fmt.Errorf("%s : no corresponding diff index rows", sheetName)
	}

	baseSheet, err := sm.newSheet(targetBaseIndexRows[0]["key"].(string), sheetName)
	if err != nil {
		return err
	}
	diffSheet, err := sm.newSheet(targetDiffIndexRows[0]["key"].(string), sheetName)
	if err != nil {
		return err
	}

	log.Printf("%s : start check", sheetName)

	dupIDs := baseSheet.dupCheckByColumn(diffSheet, "id")
	if 0 < len(dupIDs) {
		return fmt.Errorf("%s : id duplication %v", sheetName, dupIDs)
	}

	log.Printf("%s : finish check", sheetName)

	return baseSheet.merge(diffSheet)
}

type sheet struct {
	sheetKey       string
	sheetName      string
	headersArr     []string
	values         [][]interface{}
	rowsMap        []map[string]interface{}
	sheetsService  *sheets.Service
	indexSheetName string
}

func (s *sheet) headers() []string {
	if s.headersArr != nil {
		return s.headersArr
	}

	r := make([]string, len(s.values[0]))
	for i, v := range s.values[0] {
		r[i] = v.(string)
	}
	s.headersArr = r
	return r
}

func (s *sheet) headerIndexes() []int {
	r := []int{}
	for i, v := range s.values[0] {
		if v != "" {
			r = append(r, i)
		}
	}
	return r
}

func (s *sheet) rows() []map[string]interface{} {
	if s.rowsMap != nil {
		return s.rowsMap
	}

	r := []map[string]interface{}{}
	headers := s.headers()
	indexes := s.headerIndexes()
	for _, rows := range s.values[1:] {
		t := make(map[string]interface{}, len(indexes))
		for _, k := range indexes {
			if len(rows) <= k {
				t[headers[k]] = ""
			} else {
				t[headers[k]] = rows[k]
			}
		}
		if t["id"] != "" {
			r = append(r, t)
		}
	}
	s.rowsMap = r
	return r
}

func (s *sheet) headersToValueRange(headers ...string) *sheets.ValueRange {
	rows := s.rows()
	r := make([][]interface{}, len(rows))
	for i, row := range rows {
		t := make([]interface{}, len(headers))
		for j, h := range headers {
			if h != "" {
				t[j] = row[h]
			} else {
				t[j] = ""
			}
		}
		r[i] = t
	}
	return &sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         r,
	}
}

func (s *sheet) merge(diff *sheet) error {
	_, err := s.sheetsService.Spreadsheets.Values.Append(
		s.sheetKey,
		fmt.Sprintf("%s!A%d", s.sheetName, len(s.values)+2),
		diff.headersToValueRange(s.headers()...),
	).ValueInputOption("USER_ENTERED").Do()
	if err != nil {
		return errors.Wrapf(err, "sheet append failed. key:%s", s.sheetKey)
	}
	return nil
}

func (s *sheet) dupCheckByColumn(diff *sheet, column string) []interface{} {
	count := map[interface{}]int{}
	for _, v := range s.rows() {
		if i, ok := count[v[column]]; ok {
			count[v[column]] = i + 1
		} else {
			count[v[column]] = 1
		}
	}
	for _, v := range diff.rows() {
		if i, ok := count[v[column]]; ok {
			count[v[column]] = i + 1
		} else {
			count[v[column]] = 1
		}
	}
	r := []interface{}{}
	for k, v := range count {
		if 1 < v {
			r = append(r, k)
		}
	}
	return r
}

func (s *sheet) uniqueValuesByColumn(column string) []string {
	countMap := map[string]struct{}{}
	res := []string{}

	for _, row := range s.rows() {
		s := row[column].(string)
		if _, ok := countMap[s]; !ok {
			res = append(res, s)
			countMap[s] = struct{}{}
		}
	}

	return res
}

func (s *sheet) headerIndexByColumn(column string) int {
	for i, v := range s.values[0] {
		if v.(string) == column {
			return i
		}
	}
	return -1
}

func (s *sheet) replaceByColumn(column string, replaceMap map[string]string) error {
	index := s.headerIndexByColumn(column)
	update := [][]interface{}{}

	for _, values := range s.values {
		k := values[index]
		if v, ok := replaceMap[k.(string)]; ok {
			update = append(update, []interface{}{v})
		} else {
			update = append(update, []interface{}{k})
		}
	}

	_, err := s.sheetsService.Spreadsheets.Values.Update(
		s.sheetKey,
		fmt.Sprintf("%s!%s:%s", s.indexSheetName, n2c(index+1), n2c(index+1)),
		&sheets.ValueRange{
			MajorDimension: "ROWS",
			Values:         update,
		},
	).ValueInputOption("USER_ENTERED").Do()

	if err != nil {
		return errors.Wrapf(err, "sheet update failed. key:%s", s.sheetKey)
	}

	return nil
}

func (s *sheet) refresh() error {
	r, err := s.sheetsService.Spreadsheets.Values.Get(s.sheetKey, s.sheetName).Do()
	if err != nil {
		return errors.Wrapf(
			err,
			"fetch failed. key:%s sheetname:%s",
			s.sheetKey, s.sheetName,
		)
	}
	s.values = r.Values
	s.rowsMap = nil
	s.headersArr = nil
	return nil
}

func n2c(i int) string {
	j := 0
	r := ""
	for {
		i = i - 1
		j = i % 26
		i = i / 26
		if 0 < i {
			r = fmt.Sprintf("%s%s", string('A'+j), r)
		} else {
			break
		}
	}
	return fmt.Sprintf("%s%s", string('A'+j), r)
}
