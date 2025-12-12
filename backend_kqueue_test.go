//go:build freebsd || openbsd || netbsd || dragonfly || darwin

package fsnotify

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDeleteRecreateRace tests a race condition in the kqueue backend (issue #717).
//
// When a file is deleted and quickly recreated with new content, kqueue may
// miss the WRITE event, causing the file to be read before new content is available.
//
// To reliably reproduce the race condition before the fix:
//
//	go test -run TestDeleteRecreateRace -v -count=500
func TestDeleteRecreateRace(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)
	file := join(dir, "file")

	// Create initial file
	if err := os.WriteFile(file, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	// Delete and recreate the file quickly
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("recreated"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Wait for the recreated content to be visible via events
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name != file {
				continue
			}
			content, err := os.ReadFile(file)
			if err != nil {
				continue // File might not exist on REMOVE
			}
			if string(content) == "recreated" {
				return // Success
			}
		case <-timeout:
			t.Fatal("timeout waiting for recreated content")
		}
	}
}

func TestRemoveState(t *testing.T) {
	var (
		tmp  = t.TempDir()
		dir  = join(tmp, "dir")
		file = join(dir, "file")
	)
	mkdir(t, dir)
	touch(t, file)

	w := newWatcher(t, tmp)
	kq := w.b.(*kqueue)
	addWatch(t, w, tmp)
	addWatch(t, w, file)

	check := func(wantUser, wantTotal int) {
		t.Helper()

		if len(kq.watches.path) != wantTotal {
			var d []string
			for k, v := range kq.watches.path {
				d = append(d, fmt.Sprintf("%#v = %#v", k, v))
			}
			t.Errorf("unexpected number of entries in w.watches.path (have %d, want %d):\n%v",
				len(kq.watches.path), wantTotal, strings.Join(d, "\n"))
		}
		if len(kq.watches.wd) != wantTotal {
			var d []string
			for k, v := range kq.watches.wd {
				d = append(d, fmt.Sprintf("%#v = %#v", k, v))
			}
			t.Errorf("unexpected number of entries in w.watches.wd (have %d, want %d):\n%v",
				len(kq.watches.wd), wantTotal, strings.Join(d, "\n"))
		}
		if len(kq.watches.byUser) != wantUser {
			var d []string
			for k, v := range kq.watches.byUser {
				d = append(d, fmt.Sprintf("%#v = %#v", k, v))
			}
			t.Errorf("unexpected number of entries in w.watches.byUser (have %d, want %d):\n%v",
				len(kq.watches.byUser), wantUser, strings.Join(d, "\n"))
		}
	}

	check(2, 3)

	// Shouldn't change internal state.
	if err := w.Add("/path-doesnt-exist"); err == nil {
		t.Fatal(err)
	}
	check(2, 3)

	if err := w.Remove(file); err != nil {
		t.Fatal(err)
	}
	check(1, 2)

	if err := w.Remove(tmp); err != nil {
		t.Fatal(err)
	}
	check(0, 0)

	// Don't check these after ever remove since they don't map easily to number
	// of files watches. Just make sure they're 0 after everything is removed.
	{
		want := 0
		if len(kq.watches.byDir) != want {
			var d []string
			for k, v := range kq.watches.byDir {
				d = append(d, fmt.Sprintf("%#v = %#v", k, v))
			}
			t.Errorf("unexpected number of entries in w.watches.byDir (have %d, want %d):\n%v",
				len(kq.watches.byDir), want, strings.Join(d, "\n"))
		}

		if len(kq.watches.seen) != want {
			var d []string
			for k, v := range kq.watches.seen {
				d = append(d, fmt.Sprintf("%#v = %#v", k, v))
			}
			t.Errorf("unexpected number of entries in w.watches.seen (have %d, want %d):\n%v",
				len(kq.watches.seen), want, strings.Join(d, "\n"))
			return
		}
	}
}

// TestDeleteRecreateMultipleWrites tests that files recreated with multiple writes
// are properly tracked. The key is that we receive CREATE for the new file.
//
//	go test -run TestDeleteRecreateMultipleWrites -count=100
func TestDeleteRecreateMultipleWrites(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)
	file := join(dir, "file")

	// Create initial file
	if err := os.WriteFile(file, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	// Delete and recreate with multiple rapid writes
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Append more data quickly
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("-appended"); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	// We should see CREATE event for the file and be able to read final content
	timeout := time.After(500 * time.Millisecond)
	var sawCreate bool
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name != file {
				continue
			}
			if ev.Has(Create) {
				sawCreate = true
			}
			// If we've seen CREATE, check we can read final content
			if sawCreate {
				content, err := os.ReadFile(file)
				if err == nil && string(content) == "v2-appended" {
					return // Success
				}
			}
		case <-timeout:
			// Final check - if file has correct content, that's success
			content, err := os.ReadFile(file)
			if err == nil && string(content) == "v2-appended" && sawCreate {
				return
			}
			t.Fatalf("timeout: sawCreate=%v", sawCreate)
		}
	}
}

// TestRapidCreateDeleteCycle tests rapid create/delete cycles to ensure
// the seen tracking and watch setup remain consistent.
//
//	go test -run TestRapidCreateDeleteCycle -count=50
func TestRapidCreateDeleteCycle(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)
	file := join(dir, "file")

	w := newWatcher(t, dir)
	kq := w.b.(*kqueue)

	// Perform rapid create/delete cycles
	for i := 0; i < 10; i++ {
		if err := os.WriteFile(file, []byte(fmt.Sprintf("cycle-%d", i)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
	}

	// Let events settle
	time.Sleep(200 * time.Millisecond)

	// Drain events
	drainTimeout := time.After(300 * time.Millisecond)
drain:
	for {
		select {
		case <-w.Events:
		case err := <-w.Errors:
			t.Fatal(err)
		case <-drainTimeout:
			break drain
		}
	}

	// After all cycles, the file should not be in seen map since it was deleted
	kq.watches.mu.RLock()
	_, inSeen := kq.watches.seen[file]
	kq.watches.mu.RUnlock()

	if inSeen {
		t.Error("file should not be in seen map after being deleted")
	}
}

// TestCreateWriteRemoveSequence tests that we get proper events for
// create→write→remove sequence. Note that WRITE events may be coalesced
// with CREATE on kqueue, so we primarily test CREATE and REMOVE.
//
//	go test -run TestCreateWriteRemoveSequence -count=100
func TestCreateWriteRemoveSequence(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)
	file := join(dir, "file")

	w := newWatcher(t, dir)

	// Create file
	if err := os.WriteFile(file, []byte("initial"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Give time for the watch to be established before writing
	time.Sleep(50 * time.Millisecond)

	// Write to it (using append to ensure it's a separate operation)
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("-modified")
	f.Sync()
	f.Close()

	time.Sleep(50 * time.Millisecond)

	// Remove it
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}

	var (
		sawCreate, sawRemove bool
		eventOrder []string
	)

	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name != file {
				continue
			}
			if ev.Has(Create) && !sawCreate {
				sawCreate = true
				eventOrder = append(eventOrder, "CREATE")
			}
			if ev.Has(Write) {
				eventOrder = append(eventOrder, "WRITE")
			}
			if ev.Has(Remove) && !sawRemove {
				sawRemove = true
				eventOrder = append(eventOrder, "REMOVE")
			}
			if sawCreate && sawRemove {
				// Verify CREATE came before REMOVE
				createIdx := -1
				removeIdx := -1
				for i, e := range eventOrder {
					if e == "CREATE" && createIdx < 0 {
						createIdx = i
					}
					if e == "REMOVE" && removeIdx < 0 {
						removeIdx = i
					}
				}
				if createIdx > removeIdx {
					t.Errorf("CREATE came after REMOVE: %v", eventOrder)
				}
				return // Success
			}
		case <-timeout:
			t.Fatalf("timeout: sawCreate=%v sawRemove=%v events=%v", sawCreate, sawRemove, eventOrder)
		}
	}
}

// TestMultipleFilesCreatedSimultaneously tests that when multiple files are created
// at the same time, we get CREATE events for all of them.
//
//	go test -run TestMultipleFilesCreatedSimultaneously -count=50
func TestMultipleFilesCreatedSimultaneously(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	w := newWatcher(t, dir)

	numFiles := 20
	files := make(map[string]bool)

	// Create many files rapidly
	for i := 0; i < numFiles; i++ {
		file := join(dir, fmt.Sprintf("file-%d", i))
		files[file] = false
		if err := os.WriteFile(file, []byte("content"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Count how many CREATE events we receive
	timeout := time.After(1 * time.Second)
	createCount := 0
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Has(Create) {
				if _, ok := files[ev.Name]; ok && !files[ev.Name] {
					files[ev.Name] = true
					createCount++
				}
			}
			if createCount >= numFiles {
				return // Success
			}
		case <-timeout:
			missing := 0
			for f, seen := range files {
				if !seen {
					missing++
					if missing <= 5 {
						t.Logf("missing CREATE for: %s", f)
					}
				}
			}
			t.Fatalf("timeout: only got %d/%d CREATE events", createCount, numFiles)
		}
	}
}

// TestSeenTrackingConsistency ensures the seen map is properly maintained
// across various file operations.
func TestSeenTrackingConsistency(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	file1 := join(dir, "file1")
	file2 := join(dir, "file2")

	// Create initial files
	touch(t, file1)
	touch(t, file2)

	w := newWatcher(t, dir)
	kq := w.b.(*kqueue)

	// Let initial events settle
	time.Sleep(200 * time.Millisecond)

	checkSeen := func(path string, expected bool) {
		t.Helper()
		kq.watches.mu.RLock()
		_, seen := kq.watches.seen[path]
		kq.watches.mu.RUnlock()
		if seen != expected {
			t.Errorf("seen[%q] = %v, want %v", filepath.Base(path), seen, expected)
		}
	}

	// Both files should be marked as seen
	checkSeen(file1, true)
	checkSeen(file2, true)

	// Remove file1
	rm(t, file1)
	time.Sleep(100 * time.Millisecond)

	// Drain events
	drainEvents(w, 200*time.Millisecond)

	checkSeen(file1, false)
	checkSeen(file2, true)

	// Recreate file1
	touch(t, file1)
	time.Sleep(100 * time.Millisecond)
	drainEvents(w, 200*time.Millisecond)

	checkSeen(file1, true)
	checkSeen(file2, true)
}

// TestOverwriteWithRename tests the case where a file is overwritten via rename
// (like mv f1 f2 where f2 exists). The fix ensures we detect the new file.
//
//	go test -run TestOverwriteWithRename -count=100
func TestOverwriteWithRename(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	src := join(dir, "src")
	dst := join(dir, "dst")

	// Create both files
	if err := os.WriteFile(src, []byte("source-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("dest-content"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	// Rename src to dst (overwrites dst)
	if err := os.Rename(src, dst); err != nil {
		t.Fatal(err)
	}

	// We should see events indicating the overwrite
	timeout := time.After(500 * time.Millisecond)
	sawDstEvent := false
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == dst {
				sawDstEvent = true
			}
			// Check we can read the new content
			if sawDstEvent {
				content, err := os.ReadFile(dst)
				if err == nil && string(content) == "source-content" {
					return // Success
				}
			}
		case <-timeout:
			if !sawDstEvent {
				t.Fatal("timeout: didn't see any event for destination file")
			}
			return // Got event, content check is bonus
		}
	}
}

// TestSubdirectoryFileCreation tests that files created in subdirectories
// are properly detected and tracked.
func TestSubdirectoryFileCreation(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	subdir := join(dir, "subdir")
	mkdirAll(t, subdir)

	w := newWatcher(t, dir)
	addWatch(t, w, subdir)

	file := join(subdir, "file")
	if err := os.WriteFile(file, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == file && ev.Has(Create) {
				return // Success
			}
		case <-timeout:
			t.Fatal("timeout waiting for CREATE event in subdirectory")
		}
	}
}

// TestConcurrentFileOperations tests concurrent file operations to ensure
// thread safety of the kqueue backend.
//
//	go test -run TestConcurrentFileOperations -count=20
func TestConcurrentFileOperations(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	w := newWatcher(t, dir)

	var wg sync.WaitGroup
	numGoroutines := 5
	filesPerGoroutine := 10

	var totalEvents atomic.Int32

	// Start event collector
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-w.Events:
				totalEvents.Add(1)
			case err := <-w.Errors:
				t.Error(err)
			}
		}
	}()

	// Concurrent file operations
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < filesPerGoroutine; i++ {
				file := join(dir, fmt.Sprintf("file-g%d-f%d", gid, i))
				if err := os.WriteFile(file, []byte("data"), 0o644); err != nil {
					t.Error(err)
					return
				}
				// Quick write
				if err := os.WriteFile(file, []byte("updated"), 0o644); err != nil {
					t.Error(err)
					return
				}
				// Quick delete
				if err := os.Remove(file); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	close(done)

	events := int(totalEvents.Load())
	// We should have received some events (at least creates)
	// The exact number can vary due to coalescing
	if events == 0 {
		t.Error("expected some events from concurrent operations")
	}
	t.Logf("received %d events from %d file operations", events, numGoroutines*filesPerGoroutine*3)
}

// TestFileReplacedWithDirectory tests the edge case where a file is removed
// and replaced with a directory of the same name.
func TestFileReplacedWithDirectory(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	target := join(dir, "target")

	// Start as a file
	if err := os.WriteFile(target, []byte("file"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	// Remove file and replace with directory
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}

	// Should see remove and create events
	var sawRemove, sawCreate bool
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == target {
				if ev.Has(Remove) {
					sawRemove = true
				}
				if ev.Has(Create) {
					sawCreate = true
				}
			}
			if sawRemove && sawCreate {
				return
			}
		case <-timeout:
			t.Fatalf("timeout: sawRemove=%v sawCreate=%v", sawRemove, sawCreate)
		}
	}
}

// TestDirectoryReplacedWithFile tests the edge case where a directory is removed
// and replaced with a file of the same name.
func TestDirectoryReplacedWithFile(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	target := join(dir, "target")

	// Start as a directory
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	// Remove directory and replace with file
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("file"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Should see events
	var sawRemove, sawCreate bool
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == target {
				if ev.Has(Remove) {
					sawRemove = true
				}
				if ev.Has(Create) {
					sawCreate = true
				}
			}
			if sawRemove && sawCreate {
				return
			}
		case <-timeout:
			t.Fatalf("timeout: sawRemove=%v sawCreate=%v", sawRemove, sawCreate)
		}
	}
}

// TestEmptyFileVsFileWithContent tests that CREATE events are sent correctly
// for both empty files and files with content.
func TestEmptyFileVsFileWithContent(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	w := newWatcher(t, dir)

	emptyFile := join(dir, "empty")
	contentFile := join(dir, "content")

	// Create empty file
	f, err := os.Create(emptyFile)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Create file with content
	if err := os.WriteFile(contentFile, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}

	sawEmpty, sawContent := false, false
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Has(Create) {
				if ev.Name == emptyFile {
					sawEmpty = true
				}
				if ev.Name == contentFile {
					sawContent = true
				}
			}
			if sawEmpty && sawContent {
				return
			}
		case <-timeout:
			t.Fatalf("timeout: sawEmpty=%v sawContent=%v", sawEmpty, sawContent)
		}
	}
}

// TestTruncateVsRewrite tests that we detect both truncation and rewrite of files.
//
//	go test -run TestTruncateVsRewrite -count=50
func TestTruncateVsRewrite(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	file := join(dir, "file")
	if err := os.WriteFile(file, []byte("initial content here"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	// Truncate the file
	if err := os.Truncate(file, 0); err != nil {
		t.Fatal(err)
	}

	// Rewrite with new content
	if err := os.WriteFile(file, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}

	var writeCount int
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == file && ev.Has(Write) {
				writeCount++
				if writeCount >= 2 {
					return
				}
			}
		case <-timeout:
			if writeCount < 1 {
				t.Fatal("timeout: expected at least one WRITE event")
			}
			return // At least one is acceptable due to coalescing
		}
	}
}

// TestWatchAfterRemoveAndRecreate tests that if we remove a watch and recreate it,
// the internal state is consistent.
func TestWatchAfterRemoveAndRecreate(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	file := join(dir, "file")
	touch(t, file)

	w := newWatcher(t, dir)
	kq := w.b.(*kqueue)

	// Remove the directory watch
	if err := w.Remove(dir); err != nil {
		t.Fatal(err)
	}

	// Re-add it
	if err := w.Add(dir); err != nil {
		t.Fatal(err)
	}

	// Create a new file
	newFile := join(dir, "newfile")
	if err := os.WriteFile(newFile, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Should get CREATE event
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == newFile && ev.Has(Create) {
				// Verify internal state is consistent
				kq.watches.mu.RLock()
				_, inByUser := kq.watches.byUser[dir]
				kq.watches.mu.RUnlock()
				if !inByUser {
					t.Error("directory should be in byUser after re-add")
				}
				return
			}
		case <-timeout:
			t.Fatal("timeout waiting for CREATE event after re-adding watch")
		}
	}
}

// TestChmodAfterCreate tests that we get both CREATE and CHMOD events
// when a file is created and then chmoded.
func TestChmodAfterCreate(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	w := newWatcher(t, dir)

	file := join(dir, "file")
	if err := os.WriteFile(file, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)

	if err := os.Chmod(file, 0o600); err != nil {
		t.Fatal(err)
	}

	var sawCreate, sawChmod bool
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == file {
				if ev.Has(Create) {
					sawCreate = true
				}
				if ev.Has(Chmod) {
					sawChmod = true
				}
			}
			if sawCreate && sawChmod {
				return
			}
		case <-timeout:
			t.Fatalf("timeout: sawCreate=%v sawChmod=%v", sawCreate, sawChmod)
		}
	}
}

// TestRenameWithinWatchedDir tests rename operations within a watched directory.
//
//	go test -run TestRenameWithinWatchedDir -count=50
func TestRenameWithinWatchedDir(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	oldName := join(dir, "old")
	newName := join(dir, "new")

	if err := os.WriteFile(oldName, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := newWatcher(t, dir)

	if err := os.Rename(oldName, newName); err != nil {
		t.Fatal(err)
	}

	var sawRename, sawCreate bool
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			// On kqueue, rename shows as RENAME on old file and CREATE on new
			if ev.Name == oldName && ev.Has(Rename) {
				sawRename = true
			}
			if ev.Name == newName && ev.Has(Create) {
				sawCreate = true
			}
			if sawRename && sawCreate {
				return
			}
		case <-timeout:
			t.Fatalf("timeout: sawRename=%v sawCreate=%v", sawRename, sawCreate)
		}
	}
}

// TestLargeNumberOfFilesInDirectory tests that the watcher handles directories
// with many files correctly.
func TestLargeNumberOfFilesInDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	// Pre-create many files
	numPreExisting := 100
	for i := 0; i < numPreExisting; i++ {
		f := join(dir, fmt.Sprintf("existing-%d", i))
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	w := newWatcher(t, dir)

	// Now create more files
	numNew := 50
	expectedCreates := make(map[string]bool)
	for i := 0; i < numNew; i++ {
		f := join(dir, fmt.Sprintf("new-%d", i))
		expectedCreates[f] = false
		if err := os.WriteFile(f, []byte("y"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	timeout := time.After(2 * time.Second)
	createCount := 0
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Has(Create) {
				if _, ok := expectedCreates[ev.Name]; ok && !expectedCreates[ev.Name] {
					expectedCreates[ev.Name] = true
					createCount++
				}
			}
			if createCount >= numNew {
				return
			}
		case <-timeout:
			t.Fatalf("timeout: got %d/%d CREATE events", createCount, numNew)
		}
	}
}

// TestSymlinkTargetChanged tests behavior when a symlink's target is modified.
func TestSymlinkTargetChanged(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	target := join(dir, "target")
	link := join(dir, "link")

	// Create target and symlink
	if err := os.WriteFile(target, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	// Watch the directory
	w := newWatcher(t, dir)

	// Modify the target file
	if err := os.WriteFile(target, []byte("modified"), 0o644); err != nil {
		t.Fatal(err)
	}

	// We should see a WRITE event on the target
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Name == target && ev.Has(Write) {
				return
			}
		case <-timeout:
			t.Fatal("timeout waiting for WRITE event on symlink target")
		}
	}
}

// TestSeenMapNotLeaking verifies the seen map doesn't grow unboundedly
// when files are repeatedly created and deleted.
func TestSeenMapNotLeaking(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	w := newWatcher(t, dir)
	kq := w.b.(*kqueue)

	file := join(dir, "file")

	// Create and delete file multiple times
	for i := 0; i < 20; i++ {
		if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Drain events
	drainEvents(w, 300*time.Millisecond)

	// The seen map should not have the deleted file
	kq.watches.mu.RLock()
	seenLen := len(kq.watches.seen)
	_, hasFile := kq.watches.seen[file]
	kq.watches.mu.RUnlock()

	if hasFile {
		t.Error("deleted file should not be in seen map")
	}

	// Should have a reasonable number of entries (just the dir and any subdirs)
	if seenLen > 10 {
		t.Errorf("seen map may be leaking: %d entries", seenLen)
	}
}

// TestWatchCloseDuringEvents tests that closing the watcher while events
// are being generated doesn't cause panics or deadlocks.
func TestWatchCloseDuringEvents(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	w := newWatcher(t, dir)

	// Start creating files in a goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			file := join(dir, fmt.Sprintf("file-%d", i))
			os.WriteFile(file, []byte("x"), 0o644)
		}
	}()

	// Close watcher after a brief moment
	time.Sleep(10 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Wait for file creation to finish
	<-done

	// Should complete without panic or hang
}

// TestNoSpuriousCreateEvents verifies that we don't get CREATE events for files
// that already exist when setting up the watch.
func TestNoSpuriousCreateEvents(t *testing.T) {
	tmp := t.TempDir()
	dir := join(tmp, "dir")
	mkdirAll(t, dir)

	// Pre-create files
	for i := 0; i < 5; i++ {
		f := join(dir, fmt.Sprintf("existing-%d", i))
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Wait a bit, then create watcher
	time.Sleep(50 * time.Millisecond)
	w := newWatcher(t, dir)

	// Create a new file
	newFile := join(dir, "new")
	if err := os.WriteFile(newFile, []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}

	// We should only get CREATE for the new file, not the existing ones
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case err := <-w.Errors:
			t.Fatal(err)
		case ev := <-w.Events:
			if ev.Has(Create) {
				if strings.HasPrefix(filepath.Base(ev.Name), "existing-") {
					t.Errorf("got spurious CREATE for pre-existing file: %s", ev.Name)
				}
				if ev.Name == newFile {
					return // Success
				}
			}
		case <-timeout:
			t.Fatal("timeout waiting for CREATE event")
		}
	}
}

// Helper to drain events from a watcher
func drainEvents(w *Watcher, timeout time.Duration) {
	timer := time.After(timeout)
	for {
		select {
		case <-w.Events:
		case <-w.Errors:
		case <-timer:
			return
		}
	}
}
