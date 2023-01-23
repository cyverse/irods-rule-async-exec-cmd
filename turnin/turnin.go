package turnin

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// TurnIn is a struct to maintain turn in config and state
type TurnIn struct {
	Dir       string
	FailedDir string
}

func NewTurnIn(dir string) *TurnIn {
	return &TurnIn{
		Dir:       dir,
		FailedDir: path.Join(dir, "failed"),
	}
}

// MakeTurnInDir makes turn in dir
func (turnin *TurnIn) MakeTurnInDir() error {
	turninDirInfo, err := os.Stat(turnin.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(turnin.Dir, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a turn in dir (%s) error - %v", turnin.Dir, mkdirErr)
			}

			// okay
		} else {
			return fmt.Errorf("turn in dir (%s) error - %v", turnin.Dir, err)
		}
	}

	if turninDirInfo != nil {
		if !turninDirInfo.IsDir() {
			return fmt.Errorf("turn in dir (%s) exist, but not a directory", turnin.Dir)
		}

		turninDirPerm := turninDirInfo.Mode().Perm()
		if turninDirPerm&0200 != 0200 {
			return fmt.Errorf("turn in dir (%s) exist, but does not have write permission", turnin.Dir)
		}
	}

	failedTurninDirInfo, err := os.Stat(turnin.FailedDir)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(turnin.FailedDir, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a failed turn in dir (%s) error - %v", turnin.FailedDir, mkdirErr)
			}

			// okay
		} else {
			return fmt.Errorf("failed turn in dir (%s) error - %v", turnin.FailedDir, err)
		}
	}

	if failedTurninDirInfo != nil {
		if !failedTurninDirInfo.IsDir() {
			return fmt.Errorf("failed turn in dir (%s) exist, but not a directory", turnin.FailedDir)
		}

		failedTurninDirPerm := failedTurninDirInfo.Mode().Perm()
		if failedTurninDirPerm&0200 != 0200 {
			return fmt.Errorf("failed turn in dir (%s) exist, but does not have write permission", turnin.FailedDir)
		}
	}

	return nil
}

// Turnin turns a request in
func (turnin *TurnIn) Turnin(item TurnInItem) error {
	// save as a file
	id := strconv.FormatInt(time.Now().UnixMicro(), 10)
	pid := os.Getpid()
	filename := fmt.Sprintf("%s-%d", id, pid)
	turninFilePath := filepath.Join(turnin.Dir, filename)

	return item.SaveToFile(turninFilePath)
}

// Scrape finds all turn-ins
func (turnin *TurnIn) Scrape() ([]TurnInItem, error) {
	files, err := os.ReadDir(turnin.Dir)
	if err != nil {
		return nil, err
	}

	items := []TurnInItem{}
	for _, file := range files {
		if !file.IsDir() {
			fullpath := filepath.Join(turnin.Dir, file.Name())
			item, reqErr := NewTurnInRequestFromFile(fullpath)
			if reqErr != nil {
				err = reqErr
				failDirFile := filepath.Join(turnin.FailedDir, filepath.Base(fullpath))
				os.Rename(fullpath, failDirFile)
				continue
			}

			items = append(items, item)
		}
	}

	// sort by file name
	sort.SliceStable(items[:], func(i int, j int) bool {
		basei := filepath.Base(items[i].GetItemFilePath())
		basej := filepath.Base(items[j].GetItemFilePath())
		return basei < basej
	})

	return items, err
}

// MarkFailed sets a turn-in failed
func (turnin *TurnIn) MarkFailed(item TurnInItem) error {
	fullpath := item.GetItemFilePath()
	if len(fullpath) > 0 {
		failDirFile := filepath.Join(turnin.FailedDir, filepath.Base(fullpath))
		return os.Rename(fullpath, failDirFile)
	}
	return nil
}

// MarkSuccess sets a turn-in success
func (turnin *TurnIn) MarkSuccess(item TurnInItem) error {
	fullpath := item.GetItemFilePath()
	if len(fullpath) > 0 {
		return os.Remove(fullpath)
	}
	return nil
}
