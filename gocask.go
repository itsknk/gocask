package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
    flagNormal    byte = 0
    flagTombstone byte = 1
)

var activeFileSize int64 = 0 // tracks the size of the active file

type FileOffset struct {
	FileID string
	Offset int64
}


// writeEntry writes a normal key→value record (with a 1-byte flag prefix).
func writeEntry(w *bufio.Writer, key, value []byte) {
	w.WriteByte(flagNormal)
	binary.Write(w, binary.BigEndian, uint32(len(key)))
	binary.Write(w, binary.BigEndian, uint32(len(value)))
	w.Write(key)
	w.Write(value)
	activeFileSize += int64(1 + 8 + len(key) + len(value))
}


// writeTombstone writes a delete marker for key.
func writeTombstone(w *bufio.Writer, key []byte) {
	w.WriteByte(flagTombstone)
	binary.Write(w, binary.BigEndian, uint32(len(key)))
	binary.Write(w, binary.BigEndian, uint32(0)) // no value
	w.Write(key)
	activeFileSize += int64(1 + 8 + len(key))
}


// Delete marks a key as deleted: writes a tombstone and updates keyDir.
func Delete(key string, f *os.File, w *bufio.Writer, keyDir map[string]FileOffset) error {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	writeTombstone(w, []byte(key))
	w.Flush()
	keyDir[key] = FileOffset{"data.txt", offset}
	return nil
}


// implies?
func ReadData() {
	// open the file
	f, err := os.Open("data.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	// defer immediately to ensure file gets closed if program quits in middle
	defer f.Close()

	// reading binary
	reader := bufio.NewReader(f)
	for {
		var keyLen uint32
		var valueLen uint32

		// read 4 bytes for key length
		err = binary.Read(reader, binary.BigEndian, &keyLen)
		if err != nil {
			if err == io.EOF {
				break // normal end of the line
			}
			fmt.Println("Error reading key length:", err) // handle errors
			return                                        // stop immediately on any read error
		}

		// read 4 bytes for value length
		err = binary.Read(reader, binary.BigEndian, &valueLen)
		if err != nil {
			if err == io.EOF {
				break // normal end of the line
			}
			fmt.Println("Error reading value length:", err) // handle errors
			return                                          // stop
		}

		key := make([]byte, keyLen)
		value := make([]byte, valueLen)

		// read exact key bytes
		_, err = io.ReadFull(reader, key)
		if err != nil {
			fmt.Println("Error reading key:", err)
			return
		}

		// read exact value bytes
		_, err = io.ReadFull(reader, value)
		if err != nil {
			fmt.Println("Error reading value:", err)
			return
		}

		// print the entry
		fmt.Printf("%s : %s\n", strings.TrimSpace(string(key)), strings.TrimSpace(string(value))) // clean up new lines
	}
}


// rotateFile rotates the active data.txt, compacts all rotated logs into a single new log,
// writes a matching .hint file, deletes the old logs, rebuilds the in‐memory index, and
// returns a Bufio writer that now points at the fresh data.txt.
func rotateFile(oldW *bufio.Writer, keyDir map[string]FileOffset) (*bufio.Writer, error) {
    // 1) flush & lock
    oldW.Flush()
    lock := flock.New("data.txt.lock")
    if err := lock.Lock(); err != nil {
        return oldW, fmt.Errorf("lock data.txt: %w", err)
    }
    defer lock.Unlock()

    // 2) rotate data.txt → data_<ts>.log
    ts := fmt.Sprintf("%d", time.Now().Unix())
    newLog := fmt.Sprintf("data_%s.log", ts)
    if err := os.Rename("data.txt", newLog); err != nil {
        return oldW, fmt.Errorf("rotate: %w", err)
    }
    fmt.Println("Renamed active file to:", newLog)

    // 3) open fresh data.txt writer
    f, err := os.OpenFile("data.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        return oldW, fmt.Errorf("open new data.txt: %w", err)
    }
    newW := bufio.NewWriter(f)
    activeFileSize = 0

    // 4) gather all rotated logs
    logs, err := filepath.Glob("data_*.log")
    if err != nil {
        return newW, fmt.Errorf("glob logs: %w", err)
    }
    sort.Slice(logs, func(i, j int) bool {
        return extractTimestamp(logs[i]) > extractTimestamp(logs[j])
    })

    // 5) compact them
    if err := mergeFiles(logs, keyDir); err != nil {
        return newW, fmt.Errorf("compact: %w", err)
    }

    // 6) install compacted_data.txt as the canonical log
    if err := os.Rename("compacted_data.txt", newLog); err != nil {
        return newW, fmt.Errorf("install: %w", err)
    }

    // 7) ppen the newly‐compacted log and scan it sequentially,
//    tracking the exact file offset for each live (non-tombstone) entry.
compF, err := os.Open(newLog)
if err != nil {
    return newW, fmt.Errorf("open compacted log: %w", err)
}
defer compF.Close()

realOffsets := make(map[string]int64)
for {
    // record the start‐of‐record offset
    off, err := compF.Seek(0, io.SeekCurrent)
    if err != nil {
        return newW, err
    }

    // read the 1-byte flag
    var flag byte
    if err := binary.Read(compF, binary.BigEndian, &flag); err == io.EOF {
        break
    } else if err != nil {
        return newW, err
    }

    // read keyLen and valLen
    var keyLen, valLen uint32
    if err := binary.Read(compF, binary.BigEndian, &keyLen); err != nil {
        return newW, err
    }
    if err := binary.Read(compF, binary.BigEndian, &valLen); err != nil {
        return newW, err
    }

    // read the key
    keyBuf := make([]byte, keyLen)
    if _, err := io.ReadFull(compF, keyBuf); err != nil {
        return newW, err
    }
    keyStr := string(keyBuf)

    if flag == flagNormal {
        // for normal entries, remember their true offset
        realOffsets[keyStr] = off
    }
    // skip over the value bytes (tombstones have valLen==0)
    if _, err := compF.Seek(int64(valLen), io.SeekCurrent); err != nil {
        return newW, err
    }
}

// 8) write out the .hint file using those realOffsets
hintName := fmt.Sprintf("data_%s.hint", ts)
hf, err := os.Create(hintName)
if err != nil {
    return newW, fmt.Errorf("create hint: %w", err)
}
for key, off := range realOffsets {
    binary.Write(hf, binary.BigEndian, uint32(len(key)))
    hf.Write([]byte(key))
    binary.Write(hf, binary.BigEndian, uint64(off))
}
hf.Close()

    // 9) cleanup old logs and hints
    for _, old := range logs {
        if old != newLog {
            os.Remove(old)
        }
    }
    oldHints, _ := filepath.Glob("data_*.hint")
    for _, h := range oldHints {
        if h != hintName {
            os.Remove(h)
        }
    }

    // 10) rebuild keyDir in-place from the fresh hint
    fresh, err := RebuildKeyDir()
    if err != nil {
        return newW, fmt.Errorf("rebuild index: %w", err)
    }
    for k := range keyDir {
        delete(keyDir, k)
    }
    for k, fo := range fresh {
        keyDir[k] = fo
    }

    return newW, nil
}


// mergeFiles now understands the 1-byte flag.
func mergeFiles(sortedFiles []string, keyDir map[string]FileOffset) error {
    // newest→oldest
    sort.Slice(sortedFiles, func(i, j int) bool {
        return extractTimestamp(sortedFiles[i]) > extractTimestamp(sortedFiles[j])
    })

    type entry struct {
        value     []byte
        tombstone bool
    }
    latest := make(map[string]entry)

    for _, filePath := range sortedFiles {
        f, err := os.Open(filePath)
        if err != nil {
            return err
        }
        reader := bufio.NewReader(f)

        for {
            // read flag
            flag, err := reader.ReadByte()
            if err == io.EOF {
                break
            } else if err != nil {
                f.Close()
                return err
            }

            // read lengths
            var keyLen, valLen uint32
            if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
                f.Close()
                return err
            }
            if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
                f.Close()
                return err
            }

            keyBuf := make([]byte, keyLen)
            if _, err := io.ReadFull(reader, keyBuf); err != nil {
                f.Close()
                return err
            }

            keyStr := strings.TrimSpace(string(keyBuf))

            // if we've already recorded anything for this key, skip
            if _, seen := latest[keyStr]; seen {
                // skip over any value bytes
                if flag == flagNormal && valLen > 0 {
                    reader.Discard(int(valLen))
                }
                continue
            }

            if flag == flagTombstone {
                // mark deletion
                latest[keyStr] = entry{nil, true}
            } else {
                // normal
                valueBuf := make([]byte, valLen)
                if _, err := io.ReadFull(reader, valueBuf); err != nil {
                    f.Close()
                    return err
                }
                latest[keyStr] = entry{valueBuf, false}
            }
        }

        f.Close()
    }

    // write compacted file: drop any tombstoned entries
    out, err := os.OpenFile("compacted_data.txt", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
    if err != nil {
        return err
    }
    w := bufio.NewWriter(out)
    for k, e := range latest {
        if e.tombstone {
            continue
        }
        writeEntry(w, []byte(k), e.value)
    }
    w.Flush()
    out.Close()
    return nil
}


// RebuildKeyDir reads all .hint files (oldest→newest) to reconstruct the index.
func RebuildKeyDir() (map[string]FileOffset, error) {
    keyDir := make(map[string]FileOffset)

    hints, err := filepath.Glob("data_*.hint")
    if err != nil {
        return nil, fmt.Errorf("glob hints: %w", err)
    }
    sort.Slice(hints, func(i, j int) bool {
        return extractTimestamp(hints[i]) < extractTimestamp(hints[j])
    })

    for _, h := range hints {
        logFile := strings.TrimSuffix(h, ".hint") + ".log"
        f, err := os.Open(h)
        if err != nil {
            return nil, fmt.Errorf("open hint %s: %w", h, err)
        }
        r := bufio.NewReader(f)
        for {
            var keyLen uint32
            if err := binary.Read(r, binary.BigEndian, &keyLen); err == io.EOF {
                break
            } else if err != nil {
                return nil, fmt.Errorf("read keyLen: %w", err)
            }
            key := make([]byte, keyLen)
            io.ReadFull(r, key)

            var off uint64
            if err := binary.Read(r, binary.BigEndian, &off); err != nil {
                return nil, fmt.Errorf("read offset: %w", err)
            }
            keyDir[string(key)] = FileOffset{FileID: logFile, Offset: int64(off)}
        }
        f.Close()
    }

    return keyDir, nil
}


// helper function to extract the timestamp from the filename
func extractTimestamp(filePath string) int64 {
	base := filepath.Base(filePath)
	var timestamp int64
	_, err := fmt.Sscanf(base, "data_%d.log", &timestamp)
	if err != nil {
		return 0 // return - if the filename doesn't match the expected format
	}
	return timestamp
}


// get now checks for tombstones.
func Get(key string, keyDir map[string]FileOffset) (string, error) {
	fo, ok := keyDir[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	f, err := os.Open(fo.FileID)
	if err != nil {
		return "", err
	}
	defer f.Close()

	f.Seek(fo.Offset, io.SeekStart)
	flag := make([]byte, 1)
	f.Read(flag)
	if flag[0] == flagTombstone {
		return "", fmt.Errorf("key '%s' was deleted", key)
	}

	var kLen, vLen uint32
	binary.Read(f, binary.BigEndian, &kLen)
	binary.Read(f, binary.BigEndian, &vLen)
	f.Seek(int64(kLen), io.SeekCurrent)

	valBuf := make([]byte, vLen)
	io.ReadFull(f, valBuf)
	return string(valBuf), nil
}


// open a file in read-write mode, create if not exists, append to end
func main() {
	const maxFileSize = 100
	f, err := os.OpenFile("data.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	reader := bufio.NewReader(os.Stdin)
	keyDir, _ := RebuildKeyDir()

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		parts := strings.Fields(strings.TrimSpace(line))
		if len(parts) == 0 {
			continue
		}
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Usage: PUT <key> <value>")
				continue
			}
			key, val := parts[1], strings.Join(parts[2:], " ")
			offset, _ := f.Seek(0, io.SeekCurrent)
			writeEntry(w, []byte(key), []byte(val))
			w.Flush()
			keyDir[key] = FileOffset{"data.txt", offset}

		case "DEL":
			if len(parts) != 2 {
				fmt.Println("Usage: DEL <key>")
				continue
			}
			if err := Delete(parts[1], f, w, keyDir); err != nil {
				fmt.Println("Delete failed:", err)
			}

		case "GET":
			if len(parts) != 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			if v, err := Get(parts[1], keyDir); err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("Value:", v)
			}

		case "EXIT":
			return

		default:
			fmt.Println("Commands: PUT, GET, DEL, EXIT")
		}

		if activeFileSize > maxFileSize {
    		fmt.Println("Rotating...")
    		var err error
    		w, err = rotateFile(w, keyDir)
    		if err != nil {
        		fmt.Println("Rotate failed:", err)
    		}
		}

	}
}
