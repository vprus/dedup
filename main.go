package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/minio/highwayhash"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Enumerate all files under 'root', compute their content hashes, and write a manifest
// with relative names and hashes into a parquet file 'manifestPath'
func checksumCommand(root string, manifestPath string) {
	paths := walk(root)
	writeParquet(manifestPath, checksumParallel(paths, root))
}

// Enumerate all files under 'root', compute their content hashes and report
// which of the hashes are found and not found in manifest file at 'manifestPath',
// created by prior invocation of 'checksumCommand'
// When 'delete' is true, delete the files that are found in the manifest
func dedupCommand(root string, manifestPath string, really bool) {
	manifest := readParquet(manifestPath)
	println("Read manifest:", len(manifest), "files")

	found := 0
	notFound := 0
	paths := walk(root)
	for c := range checksumParallel(paths, root) {
		if manifestChecksum, ok := manifest[c.relativePath]; ok && manifestChecksum == c.checksum {
			found++
			if really {
				err := os.Remove(c.absolutePath)
				if err != nil {
					panic(err)
				}
			}
		} else {
			fmt.Printf("Not found: %v\n", c.relativePath)
			notFound++
		}
	}
	fmt.Printf("Files found in manifest at the same path: %v\n", found)
	fmt.Printf("Files not found in manifest: %v\n", notFound)
	if really {
		fmt.Printf("The duplicated files were removed\n")
	} else {
		fmt.Printf("Run with -really option to really delete files\n")
	}
}

// Return a channel that will contain all the files under 'root' and then closed
func walk(root string) <-chan string {
	r := make(chan string, 10000)
	go func() {
		fileCount := 0
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				fileCount++
				r <- path
			}
			return nil
		})
		if err != nil {
			panic(err.(any))
		}
		println("Enumerated files:", fileCount)
		close(r)
	}()
	return r
}

// Given a file path, compute it's contennt hash and return base64 encoded representation of the hash
// Use highwayhash, just because
func checksumOfFile(path string) string {
	key, err := hex.DecodeString("08ed80781e731f756ad4deefa19d3691787654241f0cda6013b2b82c7c391555")
	if err != nil {
		fmt.Printf("Cannot decode hex key: %v", err) // add error handling
		return ""
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Failed to open the file: %v", err) // add error handling
		return ""
	}
	defer file.Close()

	hash, err := highwayhash.New(key)
	if err != nil {
		fmt.Printf("Failed to create HighwayHash instance: %v", err) // add error handling
		return ""
	}

	if _, err = io.Copy(hash, file); err != nil {
		fmt.Printf("Failed to read from file: %v", err) // add error handling
		return ""
	}

	checksum := hash.Sum(nil)
	return base64.StdEncoding.EncodeToString(checksum)
}

type PathChecksum struct {
	absolutePath string
	relativePath string
	checksum     string
}

// Given a channel with file paths, return channel with (relativePath, content hash)
// pairs, whcih will be closed when there are no more entries.
func checksumParallel(paths <-chan string, root string) <-chan PathChecksum {
	r := make(chan PathChecksum, 10000)
	go func() {
		checksumCount := atomic.Int32{}
		wg := &sync.WaitGroup{}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				for path := range paths {
					relative, err := filepath.Rel(root, path)
					if err != nil {
						panic(err.(any))
					}
					checksumCount.Add(1)
					r <- PathChecksum{absolutePath: path, relativePath: relative, checksum: checksumOfFile(path)}
				}
				wg.Done()
			}()
		}
		wg.Wait()
		println("Checksummed files:", checksumCount.Load())
		close(r)
	}()
	return r
}

func main() {
	app := &cli.App{
		Name:  "archiver",
		Usage: "lightweight tool to archive data",
		Commands: []*cli.Command{
			{
				Name:  "checksum",
				Usage: "Compute checksums for all files in a directory",
				Action: func(c *cli.Context) error {
					if c.Args().Len() != 2 {
						return fmt.Errorf("incorrect number of arguments for 'checksum' command")
					}

					checksumCommand(c.Args().Get(0), c.Args().Get(1))
					return nil
				},
			},
			{
				Name:  "delete",
				Usage: "delete files if identical files are found in a manifest",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name: "really",
						Value: false,
					},
				},
				Action: func(c *cli.Context) error {
					if c.Args().Len() != 2 {
						return fmt.Errorf("incorrect number of arguments for 'delete' command")
					}

					dedupCommand(c.Args().Get(0), c.Args().Get(1), c.Bool("really"))
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
		fmt.Println(app.Usage)
	}
}
